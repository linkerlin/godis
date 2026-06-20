package gnet

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/database"
)

func TestListenAndServe(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	db := database.MustNewStandaloneServer()
	server := NewGnetServer(db)
	go func() {
		_ = server.Run(addr)
	}()
	time.Sleep(2 * time.Second)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "+PONG" {
		t.Fatalf("get wrong response: %q", line)
	}
	_ = conn.Close()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGnetGracefulShutdown(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	db := database.MustNewStandaloneServer()
	server := NewGnetServer(db)
	go func() {
		_ = server.Run(addr)
	}()
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", addr)
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

	if err := server.Close(); err != nil {
		t.Fatal(err)
	}

	conn2, err := net.Dial("tcp", addr)
	if err == nil {
		defer conn2.Close()
		_ = conn2.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buf := make([]byte, 64)
		_, _ = conn2.Read(buf)
	}
}

func TestGnetCloseWaitsForInFlight(t *testing.T) {
	db := database.MustNewStandaloneServer()
	server := NewGnetServer(db)

	server.inFlight.Add(1)
	released := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		server.inFlight.Done()
		close(released)
	}()

	start := time.Now()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("Close returned too quickly (%v), expected to wait for in-flight handlers", elapsed)
	}
	<-released
}
