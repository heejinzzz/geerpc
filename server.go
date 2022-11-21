package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/heejinzzz/geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

// geerpc 报文格式：
// | Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
// | <------      固定 JSON 编码      ------->  | <-------   编码方式由 CodeType 决定   --------> |

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber    int           // MagicNumber marks this is a geerpc request
	CodecType      codec.Type    // client may choose different Codec to encode header and body
	ConnectTimeout time.Duration // connect timeout, 0 means no limit
	HandleTimeout  time.Duration // handle request time out, 0 means no limit
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// Server represents an RPC Server
type Server struct {
	serviceMap sync.Map
}

// NewServer creates a new Server
func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

// Accept accepts connections on the listener and serves requests
// for each incoming connection
func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error: ", err)
			return
		}
		go s.ServeConn(conn)
	}
}

// Accept accepts connections on the listener and serves requests
// for each incoming connection
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

// ServeConn runs the server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up
func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: decoding option error: ", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	s.ServeCodec(f(conn), opt.HandleTimeout)
}

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (s *Server) ServeCodec(cc codec.Codec, timeout time.Duration) {
	sending := new(sync.Mutex) // make sure to send a complete response
	wg := new(sync.WaitGroup)  // wait until all request are handled
	for {
		req, err := s.readRequest(cc)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, nil, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, timeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// request stores header, body of a call and the reply to be response
type request struct {
	h      *codec.Header // header of a request
	argv   reflect.Value // body of a request
	replyv reflect.Value // reply to be response
	svc    *service
	method *methodType
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (s *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := s.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := &request{h: h}
	req.svc, req.method, err = s.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}

	req.argv = req.method.newArgv()
	req.replyv = req.method.newReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	called, sent := make(chan struct{}), make(chan struct{})

	go func() {
		err := req.svc.call(req.method, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

// Register publishes the set of methods of the rcvr in the server
func (s *Server) Register(rcvr interface{}) error {
	svc := newService(rcvr)
	if _, existed := s.serviceMap.LoadOrStore(svc.name, svc); existed {
		return errors.New("rpc: service already defined: " + svc.name)
	}
	return nil
}

// Register publishes the set of methods of the rcvr in the DefaultServer
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

func (s *Server) findService(serviceMethod string) (*service, *methodType, error) {
	index := strings.LastIndex(serviceMethod, ".")
	if index < 0 {
		return nil, nil, errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
	}
	serviceName, methodName := serviceMethod[:index], serviceMethod[index+1:]
	svc, ok := s.serviceMap.Load(serviceName)
	if !ok {
		return nil, nil, errors.New("rpc server: can't find service: " + serviceName)
	}
	method := svc.(*service).methods[methodName]
	if method == nil {
		return nil, nil, errors.New("rpc server: can't find method: " + methodName)
	}
	return svc.(*service), method, nil
}

const (
	connected        = "200 Connected to Gee RPC"
	defaultRpcPath   = "/_geerpc_"
	defaultDebugPath = "/debug/geerpc"
)

// ServeHTTP implements a http.Handler that answers RPC requests
func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Println("rpc hijacking ", req.RemoteAddr, " error: ", err.Error())
		return
	}

	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath.
// It is still necessary to invoke http.Serve(), typically in a go statement
func (s *Server) HandleHTTP() {
	http.Handle(defaultRpcPath, s)
	http.Handle(defaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path: ", defaultDebugPath)
}

// HandleHTTP is a convenient approach for default server to register HTTP handlers
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
