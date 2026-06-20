package std

import (
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/tcp"
)

func TestStdServerRejectsAtMaxClients(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		tcp.ClientCounter = 0
		tcp.RejectedConnections = 0
	}()

	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()

	handler, err := MakeHandler()
	if err != nil {
		t.Fatal(err)
	}
	go tcp.ListenAndServe(listener, handler, closeChan)

	conn1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	conn2, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	_ = conn2.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, len(tcp.MaxClientsErrReply))
	n, err := conn2.Read(buf)
	if err != nil {
		t.Fatalf("read rejection reply: %v", err)
	}
	if string(buf[:n]) != string(tcp.MaxClientsErrReply) {
		t.Fatalf("unexpected rejection reply: %q", buf[:n])
	}

	_ = conn1.Close()
	closeChan <- struct{}{}
	time.Sleep(100 * time.Millisecond)
}
