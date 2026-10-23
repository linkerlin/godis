package tcp

import (
	"net"
	"testing"
	"time"

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
		applyTCPKeepAlive(srv, 60)
		applyTCPKeepAlive(srv, 0) // disabled path
		// No panic / error path is enough; period is OS-specific.
		_ = time.Second
	case <-time.After(2 * time.Second):
		t.Fatal("accept timeout")
	}
}
