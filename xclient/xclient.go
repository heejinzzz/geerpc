package xclient

import (
	"context"
	. "github.com/heejinzzz/geerpc"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *Option
	mutex   sync.Mutex // protect following
	clients map[string]*Client
}

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

func (xc *XClient) Close() error {
	xc.mutex.Lock()
	defer xc.mutex.Unlock()
	for key, c := range xc.clients {
		_ = c.Close()
		delete(xc.clients, key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mutex.Lock()
	defer xc.mutex.Unlock()
	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	c, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return c.Call(ctx, serviceMethod, args, reply)
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
// xc will choose a proper server
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	server, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(server, ctx, serviceMethod, args, reply)
}

// Broadcast invokes the named function for every server registered in discovery.
// If an error occurs in any of the instances, one of the errors is returned;
// if the call succeeds, the result of one of them is returned
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var (
		wg                sync.WaitGroup
		mu                sync.Mutex // protect e and replyDone
		e                 error
		replyDone         = reply == nil // if reply is nil, don't need to set value
		ctxCancel, cancel = context.WithCancel(ctx)
	)
	for _, server := range servers {
		wg.Add(1)
		go func(server string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.TypeOf(reply).Elem()).Interface()
			}
			err := xc.call(server, ctxCancel, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel() // if any call failed, cancel all unfinished calls
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(server)
	}
	wg.Wait()
	return e
}
