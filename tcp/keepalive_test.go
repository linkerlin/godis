package tcp

import (
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
)

func TestApplyTCPKeepAlive(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		done <- c
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	select {
	case srv := <-done:
		defer srv.Close()
		old := config.Properties
		config.Properties = &config.ServerProperties{TCPKeepAlive: 60}
		defer func() { config.Properties = old }()
		applyTCPKeepAlive(srv)
		// No panic / error path is enough; period is OS-specific.
		_ = time.Second
	case <-time.After(2 * time.Second):
		t.Fatal("accept timeout")
	}
}
