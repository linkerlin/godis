package tcp

import (
	"net"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/config"
)

func TestTryAcceptClientRespectsMaxClients(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		atomic.StoreInt32(&ClientCounter, 0)
		atomic.StoreUint64(&RejectedConnections, 0)
	}()

	if !TryAcceptClient() {
		t.Fatal("first client should be accepted")
	}
	if TryAcceptClient() {
		t.Fatal("expected second client rejected at limit")
	}
	if atomic.LoadUint64(&RejectedConnections) != 1 {
		t.Fatalf("expected 1 rejected, got %d", atomic.LoadUint64(&RejectedConnections))
	}
	ReleaseClient()
	if atomic.LoadInt32(&ClientCounter) != 0 {
		t.Fatalf("expected 0 clients after release, got %d", atomic.LoadInt32(&ClientCounter))
	}
}

func TestMaxClientsErrReplyFormat(t *testing.T) {
	if string(MaxClientsErrReply) != "-ERR max number of clients reached\r\n" {
		t.Fatalf("unexpected reply: %q", MaxClientsErrReply)
	}
}

type stubConn struct {
	net.Conn
	written []byte
	closed  bool
}

func (s *stubConn) Write(b []byte) (int, error) {
	s.written = append(s.written, b...)
	return len(b), nil
}

func (s *stubConn) Close() error {
	s.closed = true
	return nil
}

func TestRejectConnectionMaxClients(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		atomic.StoreUint64(&RejectedConnections, 0)
	}()

	conn := &stubConn{}
	RejectConnectionMaxClients(conn)
	if !conn.closed {
		t.Fatal("connection should be closed")
	}
	if string(conn.written) != string(MaxClientsErrReply) {
		t.Fatalf("unexpected write: %q", conn.written)
	}
	if atomic.LoadUint64(&RejectedConnections) != 1 {
		t.Fatalf("expected rejected count 1, got %d", atomic.LoadUint64(&RejectedConnections))
	}
}

func TestTryAcceptClientUnlimitedWhenZero(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 0}
	defer func() {
		config.Properties = old
		atomic.StoreInt32(&ClientCounter, 0)
		atomic.StoreUint64(&RejectedConnections, 0)
	}()

	for i := 0; i < 5; i++ {
		if !TryAcceptClient() {
			t.Fatalf("client %d should be accepted when maxclients is 0", i+1)
		}
	}
	if atomic.LoadInt32(&ClientCounter) != 5 {
		t.Fatalf("expected 5 clients, got %d", atomic.LoadInt32(&ClientCounter))
	}
	if atomic.LoadUint64(&RejectedConnections) != 0 {
		t.Fatalf("expected no rejections, got %d", atomic.LoadUint64(&RejectedConnections))
	}
}

func TestTryAcceptClientReacceptAfterRelease(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		atomic.StoreInt32(&ClientCounter, 0)
		atomic.StoreUint64(&RejectedConnections, 0)
	}()

	if !TryAcceptClient() {
		t.Fatal("first client should be accepted")
	}
	if TryAcceptClient() {
		t.Fatal("second client should be rejected")
	}
	ReleaseClient()
	if !TryAcceptClient() {
		t.Fatal("client should be accepted after release")
	}
	if atomic.LoadInt32(&ClientCounter) != 1 {
		t.Fatalf("expected 1 active client, got %d", atomic.LoadInt32(&ClientCounter))
	}
}

func TestGetRejectedConnections(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 1}
	defer func() {
		config.Properties = old
		atomic.StoreInt32(&ClientCounter, 0)
		atomic.StoreUint64(&RejectedConnections, 0)
	}()

	if GetRejectedConnections() != 0 {
		t.Fatalf("expected 0 initially, got %d", GetRejectedConnections())
	}
	_ = TryAcceptClient()
	_ = TryAcceptClient()
	if GetRejectedConnections() != 1 {
		t.Fatalf("expected 1 rejected, got %d", GetRejectedConnections())
	}
}
