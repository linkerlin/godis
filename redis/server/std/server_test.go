package std

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/tcp"
)

func TestListenAndServe(t *testing.T) {
	var err error
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := listener.Addr().String()
	handler, err := MakeHandler()
	if err != nil {
		t.Error(err)
		return
	}
	go tcp.ListenAndServe(listener, handler, closeChan)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.Write([]byte("PING\r\n"))
	if err != nil {
		t.Error(err)
		return
	}
	bufReader := bufio.NewReader(conn)
	line, _, err := bufReader.ReadLine()
	if err != nil {
		t.Error(err)
		return
	}
	if string(line) != "+PONG" {
		t.Error("get wrong response")
		return
	}
	closeChan <- struct{}{}
	time.Sleep(time.Second)
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	closeChan := make(chan struct{})
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := MakeHandler()
	if err != nil {
		t.Fatal(err)
	}
	go tcp.ListenAndServe(listener, handler, closeChan)
	defer func() { closeChan <- struct{}{} }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// Illegal bulk header → protocol error; Redis closes after error reply.
	if _, err := conn.Write([]byte("$abc\r\n")); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if n > 0 {
		got := string(buf[:n])
		if !strings.Contains(got, "protocol error") {
			t.Fatalf("expected protocol error reply, got %q", got)
		}
	}
	// Subsequent read should see EOF (connection closed).
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection closed after protocol error")
	}
}
