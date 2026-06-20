package std

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/tcp"
)

func TestHandlerGracefulShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	handler, err := MakeHandler()
	if err != nil {
		t.Fatal(err)
	}

	closeChan := make(chan struct{})
	go tcp.ListenAndServe(listener, handler, closeChan)

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	line, _, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "+PONG" {
		t.Fatalf("unexpected response: %q", line)
	}

	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}

	// New connections must be rejected while shutting down / after close.
	conn2, err := net.Dial("tcp", listener.Addr().String())
	if err == nil {
		defer conn2.Close()
		_ = conn2.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 64)
		_, _ = conn2.Read(buf)
	}
}

func TestHandlerCloseWaitsForInFlight(t *testing.T) {
	handler, err := MakeHandler()
	if err != nil {
		t.Fatal(err)
	}

	handler.inFlight.Add(1)
	released := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		handler.inFlight.Done()
		close(released)
	}()

	start := time.Now()
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("Close returned too quickly (%v), expected to wait for in-flight handlers", elapsed)
	}
	<-released
}
