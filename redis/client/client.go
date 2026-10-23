package client

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/sync/wait"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
)

const (
	created = iota
	running
	closed
)

// Client is a pipeline mode redis client
type Client struct {
	connMu      sync.Mutex
	conn        net.Conn
	pendingReqs chan *request // wait to send
	waitingReqs chan *request // waiting response
	ticker      *time.Ticker
	addr        string
	// done is closed by Close once; it lets Send/doHeartbeat/handleWrite
	// abandon blocked channel ops. pendingReqs is never closed — a send can
	// never race a close that doesn't happen; Close waits for nothing, so
	// there is no WaitGroup Add-vs-Wait pair either (the nightly race gate
	// caught both shapes: close(pendingReqs) vs heartbeat send, and
	// working.Wait vs doHeartbeat.Add).
	done   chan struct{}
	status int32
}

// request is a message sends to redis server
type request struct {
	id        uint64
	args      [][]byte
	reply     redis.Reply
	heartbeat bool
	waiting   *wait.Wait
	err       error
}

const (
	chanSize = 256
	maxWait  = 3 * time.Second
)

// MakeClient creates a new client
func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Client{
		addr:        addr,
		conn:        conn,
		pendingReqs: make(chan *request, chanSize),
		waitingReqs: make(chan *request, chanSize),
		done:        make(chan struct{}),
	}, nil
}

func (client *Client) RemoteAddress() string {
	return client.addr
}

// Start starts asynchronous goroutines
func (client *Client) Start() {
	client.ticker = time.NewTicker(10 * time.Second)
	go client.handleWrite()
	go client.handleRead()
	go client.heartbeat()
	atomic.StoreInt32(&client.status, running)
}

// Close stops asynchronous goroutines and close connection
func (client *Client) Close() {
	if !atomic.CompareAndSwapInt32(&client.status, running, closed) {
		atomic.StoreInt32(&client.status, closed)
		return
	}
	close(client.done) // unblock Send/doHeartbeat/heartbeat/handleWrite
	client.ticker.Stop()

	// clean
	client.connMu.Lock()
	if client.conn != nil {
		_ = client.conn.Close()
	}
	if client.waitingReqs != nil {
		close(client.waitingReqs)
		client.waitingReqs = nil
	}
	client.connMu.Unlock()
}

func (client *Client) reconnect() {
	logger.Info("reconnect with: " + client.addr)
	client.connMu.Lock()
	if client.conn != nil {
		_ = client.conn.Close() // ignore possible errors from repeated closes
	}

	var conn net.Conn
	for i := 0; i < 3; i++ {
		var err error
		conn, err = net.Dial("tcp", client.addr)
		if err != nil {
			logger.Error("reconnect error: " + err.Error())
			time.Sleep(time.Second)
			continue
		} else {
			break
		}
	}
	if conn == nil { // reach max retry, abort
		oldWaiting := client.waitingReqs
		client.waitingReqs = nil
		client.connMu.Unlock()
		if oldWaiting != nil {
			close(oldWaiting)
			for req := range oldWaiting {
				req.err = errors.New("connection closed")
				req.waiting.Done()
			}
		}
		client.Close()
		return
	}
	client.conn = conn

	oldWaiting := client.waitingReqs
	client.waitingReqs = make(chan *request, chanSize)
	client.connMu.Unlock()

	if oldWaiting != nil {
		close(oldWaiting)
		for req := range oldWaiting {
			req.err = errors.New("connection closed")
			req.waiting.Done()
		}
	}
	// restart handle read
	go client.handleRead()
}

func (client *Client) heartbeat() {
	for {
		select {
		case <-client.done:
			return
		case <-client.ticker.C:
			client.doHeartbeat()
		}
	}
}

func (client *Client) handleWrite() {
	for {
		select {
		case req := <-client.pendingReqs:
			client.doRequest(req)
		case <-client.done:
			// fail whatever was already queued, then stop
			for {
				select {
				case req := <-client.pendingReqs:
					req.err = errors.New("client closed")
					req.waiting.Done()
				default:
					return
				}
			}
		}
	}
}

// Send sends a request to redis server
func (client *Client) Send(args [][]byte) redis.Reply {
	if atomic.LoadInt32(&client.status) != running {
		return protocol.MakeErrReply("client closed")
	}
	req := &request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	req.waiting.Add(1)
	select {
	case client.pendingReqs <- req:
	case <-client.done: // Close won: abandon instead of racing close(pendingReqs)
		req.waiting.Done()
		return protocol.MakeErrReply("client closed")
	}
	timeout := req.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return protocol.MakeErrReply("server time out")
	}
	if req.err != nil {
		return protocol.MakeErrReply("request failed " + req.err.Error())
	}
	return req.reply
}

func (client *Client) doHeartbeat() {
	if atomic.LoadInt32(&client.status) != running {
		return
	}
	request := &request{
		args:      [][]byte{[]byte("PING")},
		heartbeat: true,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	select {
	case client.pendingReqs <- request:
	case <-client.done: // Close won: abandon instead of racing close(pendingReqs)
		request.waiting.Done()
	}
	if !request.waiting.WaitWithTimeout(maxWait) {
		return
	}
}

func (client *Client) doRequest(req *request) {
	if req == nil || len(req.args) == 0 {
		return
	}
	re := protocol.MakeMultiBulkReply(req.args)
	bytes := re.ToBytes()

	client.connMu.Lock()
	var err error
	for i := 0; i < 3; i++ { // only retry, waiting for handleRead
		_, err = client.conn.Write(bytes)
		if err == nil ||
			(!strings.Contains(err.Error(), "timeout") && // only retry timeout
				!strings.Contains(err.Error(), "deadline exceeded")) {
			break
		}
	}
	if err == nil && client.waitingReqs != nil {
		client.waitingReqs <- req
		client.connMu.Unlock()
	} else {
		client.connMu.Unlock()
		if err == nil {
			err = errors.New("client closed")
		}
		req.err = err
		req.waiting.Done()
	}
}

func (client *Client) finishRequest(reply redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			debug.PrintStack()
			logger.Error(err)
		}
	}()
	client.connMu.Lock()
	waitingReqs := client.waitingReqs
	client.connMu.Unlock()
	if waitingReqs == nil {
		return
	}
	request := <-waitingReqs
	if request == nil {
		return
	}
	request.reply = reply
	if request.waiting != nil {
		request.waiting.Done()
	}
}

func (client *Client) handleRead() {
	client.connMu.Lock()
	conn := client.conn
	client.connMu.Unlock()
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			status := atomic.LoadInt32(&client.status)
			if status == closed {
				return
			}
			client.reconnect()
			return
		}
		client.finishRequest(payload.Data)
	}
}
