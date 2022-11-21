package geerpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/heejinzzz/geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call represents an active RPC.
type Call struct {
	ReqID         uint64
	ServiceMethod string      // format "<service>.<method>"
	Args          interface{} // arguments to the function
	Reply         interface{} // reply from the function
	Error         error       // if error occurs, it will be set
	Done          chan *Call  // Strobes when call is complete
}

func (call *Call) done() {
	call.Done <- call
}

// Client represents an RPC Client.
// There may be multiple outstanding Calls associated
// with a single Client, and a Client may be used by
// multiple goroutines simultaneously.
type Client struct {
	cc       codec.Codec
	opt      *Option
	sending  sync.Mutex // make sure to send a complete response
	header   codec.Header
	mutex    sync.Mutex // protect following
	reqID    uint64
	pending  map[uint64]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

// newClient creates and starts a new geerpc client
func newClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error: ", err)
		return nil, err
	}
	// send options with server
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		reqID:   1, // seq starts with 1, 0 means invalid call
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

var ErrShutDown = errors.New("connection is shut down")

// Close the connection
func (c *Client) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closing {
		return ErrShutDown
	}
	c.closing = true
	return c.cc.Close()
}

// IsAvailable return true if the client does work
func (c *Client) IsAvailable() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return !c.shutdown && !c.closing
}

// add the call to client.pending and update client.reqID
func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutDown
	}
	call.ReqID = c.reqID
	c.pending[call.ReqID] = call
	c.reqID++
	return call.ReqID, nil
}

// remove the call from client.pending according to reqID and return it
func (c *Client) removeCall(reqID uint64) *Call {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	call := c.pending[reqID]
	delete(c.pending, reqID)
	return call
}

// called when an error occurs on the server or client,
// sets shutdown to true, and notifies all pending calls of the error message
func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Error = err
		call.done()
	}
}

// receive response call from server
func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.cc.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.ReqID)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed
			err = c.cc.ReadBody(nil)
		case h.Error != "":
			call.Error = errors.New(h.Error)
			err = c.cc.ReadBody(nil)
			call.done()
		default:
			err = c.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body error: " + err.Error())
			}
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	c.terminateCalls(err)
}

func parseOptions(opts []*Option) (*Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

type clientCreateResult struct {
	c   *Client
	err error
}

type newClientFunc func(conn net.Conn, opt *Option) (*Client, error)

func dialWithTimeout(f newClientFunc, network, addr string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, addr, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientCreateResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientCreateResult{c: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.c, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.c, result.err
	}
}

// Dial connects to the RPC server at the specified network address
// and create a new client
func Dial(network, addr string, opts ...*Option) (client *Client, err error) {
	return dialWithTimeout(newClient, network, addr, opts...)
}

func (c *Client) send(call *Call) {
	c.sending.Lock()
	defer c.sending.Unlock()

	// register this call
	reqID, err := c.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	c.header.ServiceMethod = call.ServiceMethod
	c.header.ReqID = reqID
	c.header.Error = ""

	// encode and send the request
	if err := c.cc.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(reqID)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Call invokes the named function, waits for it to complete,
// and returns its error status
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	done := make(chan *Call, 10)
	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	c.send(call)
	select {
	case <-ctx.Done():
		c.removeCall(call.ReqID)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case <-call.Done:
		return call.Error
	}
}

// newHTTPClient new a Client instance via HTTP as transport protocol
func newHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRpcPath))

	// Require successful HTTP response before switching to RPC protocol
	res, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && res.Status == connected {
		return newClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + res.Status)
	}
	return nil, err
}

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path
func DialHTTP(network, addr string, opts ...*Option) (*Client, error) {
	return dialWithTimeout(newHTTPClient, network, addr, opts...)
}

// XDial calls different functions to connect to an RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch {
	case protocol == "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		return Dial(protocol, addr, opts...)
	}
}
