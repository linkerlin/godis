package gnet

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/database"
	"github.com/linkerlin/godis/tcp"
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

func TestGnetMaxClientsRejection(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		tcp.ClientCounter = 0
		tcp.RejectedConnections = 0
	}()

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

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn1.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn1)
	line, _, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	if string(line) != "+PONG" {
		t.Fatalf("unexpected pong: %q", line)
	}

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(tcp.MaxClientsErrReply))
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("read rejection reply: %v", err)
	}
	if string(buf[:n]) != string(tcp.MaxClientsErrReply) {
		t.Fatalf("unexpected rejection reply: %q", buf[:n])
	}
	if tcp.GetRejectedConnections() < 1 {
		t.Fatalf("expected rejected count >= 1, got %d", tcp.GetRejectedConnections())
	}
	if tcp.ClientCounter != 1 {
		t.Fatalf("expected 1 active client, got %d", tcp.ClientCounter)
	}

	_ = conn1.Close()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
}
