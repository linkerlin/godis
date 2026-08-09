package connection

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/linkerlin/godis/lib/logger"
)

// FakeConn implements redis.Connection for test
type FakeConn struct {
	Connection
	buf         []byte
	offset      int
	waitOn      chan struct{}
	closed      bool
	mu          sync.Mutex
	fakeRemote  string // optional override for RemoteAddr()
}

func NewFakeConn() *FakeConn {
	c := &FakeConn{}
	now := time.Now()
	c.createdAt = now
	c.lastActive = now
	c.clientID = globalClientID.Add(1)
	c.localAddr = "127.0.0.1:6399"
	return c
}

// SetRemoteAddr overrides RemoteAddr for protected-mode / ACL tests.
func (c *FakeConn) SetRemoteAddr(addr string) {
	c.fakeRemote = addr
}

func (c *FakeConn) SetClusterReadOnly(v bool) { c.Connection.SetClusterReadOnly(v) }
func (c *FakeConn) IsClusterReadOnly() bool   { return c.Connection.IsClusterReadOnly() }
func (c *FakeConn) SetAsking(v bool)          { c.Connection.SetAsking(v) }
func (c *FakeConn) IsAsking() bool            { return c.Connection.IsAsking() }
func (c *FakeConn) ConsumeAsking() bool       { return c.Connection.ConsumeAsking() }

func (c *FakeConn) RemoteAddr() string {
	if c.fakeRemote != "" {
		return c.fakeRemote
	}
	return ""
}

// Write writes data to buffer
func (c *FakeConn) Write(b []byte) (int, error) {
	if c.closed {
		return 0, io.EOF
	}
	c.mu.Lock()
	c.buf = append(c.buf, b...)
	c.mu.Unlock()
	c.notify()
	return len(b), nil
}

func (c *FakeConn) notify() {
	if c.waitOn != nil {
		c.mu.Lock()
		if c.waitOn != nil {
			logger.Debug(fmt.Sprintf("notify %p", c.waitOn))
			close(c.waitOn)
			c.waitOn = nil
		}
		c.mu.Unlock()
	}
}

func (c *FakeConn) wait(offset int) {
	c.mu.Lock()
	if c.offset != offset { // new data during waiting lock
		return
	}
	if c.waitOn == nil {
		c.waitOn = make(chan struct{})
	}
	waitOn := c.waitOn
	logger.Debug(fmt.Sprintf("wait on %p", waitOn))
	c.mu.Unlock()
	<-waitOn
	logger.Debug(fmt.Sprintf("wait on %p finish", waitOn))
}

// Read reads data from buffer
func (c *FakeConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	n := copy(p, c.buf[c.offset:])
	c.offset += n
	offset := c.offset
	c.mu.Unlock()
	if n == 0 {
		if c.closed {
			return n, io.EOF
		}
		c.wait(offset)
		// after notify
		if c.closed {
			return n, io.EOF
		}
		n = copy(p, c.buf[c.offset:])
		c.offset += n
		return n, nil
	}
	if c.closed {
		return n, io.EOF
	}
	return n, nil
}

// Clean resets the buffer
func (c *FakeConn) Clean() {
	c.waitOn = make(chan struct{})
	c.buf = nil
	c.offset = 0
}

// Bytes returns written data
func (c *FakeConn) Bytes() []byte {
	return c.buf
}

func (c *FakeConn) Close() error {
	c.closed = true
	c.notify()
	return nil
}
