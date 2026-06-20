package tcp

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
)

func TestListenAndServeRejectsAtMaxClients(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		ClientCounter = 0
		RejectedConnections = 0
	}()

	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	go ListenAndServe(listener, MakeEchoHandler(), closeChan)

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()

	time.Sleep(50 * time.Millisecond)

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	_ = conn2.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, len(MaxClientsErrReply))
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("read rejection reply: %v", err)
	}
	if string(buf[:n]) != string(MaxClientsErrReply) {
		t.Fatalf("unexpected rejection reply: %q", buf[:n])
	}
	if GetRejectedConnections() != 1 {
		t.Fatalf("expected 1 rejected connection, got %d", GetRejectedConnections())
	}

	_ = conn1.Close()
	time.Sleep(50 * time.Millisecond)

	conn3, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn3.Close()
	if _, err = conn3.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn3)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "hello\n" {
		t.Fatalf("expected echo hello, got %q", line)
	}

	closeChan <- struct{}{}
	time.Sleep(100 * time.Millisecond)
}
