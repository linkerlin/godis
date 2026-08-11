package connection

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestFakeConnBasics(t *testing.T) {
	c := NewFakeConn()
	if c.GetClientID() == 0 {
		t.Fatal("expected non-zero client id")
	}
	if c.LocalAddr() != "127.0.0.1:6399" {
		t.Fatalf("LocalAddr=%q", c.LocalAddr())
	}
	if c.RemoteAddr() != "" {
		t.Fatalf("default RemoteAddr=%q", c.RemoteAddr())
	}
	c.SetRemoteAddr("10.0.0.1:1234")
	if c.RemoteAddr() != "10.0.0.1:1234" {
		t.Fatalf("RemoteAddr=%q", c.RemoteAddr())
	}

	n, err := c.Write([]byte("hello"))
	if err != nil || n != 5 {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	if !bytes.Equal(c.Bytes(), []byte("hello")) {
		t.Fatalf("Bytes=%q", c.Bytes())
	}
	c.Clean()
	if len(c.Bytes()) != 0 {
		t.Fatalf("Clean left %q", c.Bytes())
	}

	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("y")); !errors.Is(err, io.EOF) {
		t.Fatalf("Write after Close: %v", err)
	}
}

func TestConnectionIdentityAndFlags(t *testing.T) {
	c := NewFakeConn()

	c.SetClientName("cli")
	if c.GetClientName() != "cli" || c.Name() != "cli" {
		t.Fatalf("name=%q GetClientName=%q", c.Name(), c.GetClientName())
	}
	c.SetTrackingID("tid")
	if c.GetTrackingID() != "tid" {
		t.Fatal(c.GetTrackingID())
	}
	c.SetLastCommand("get")
	if c.GetLastCommand() != "get" {
		t.Fatal(c.GetLastCommand())
	}
	c.SetLibName("go")
	c.SetLibVer("1.0")
	if c.GetLibName() != "go" || c.GetLibVer() != "1.0" {
		t.Fatal("lib name/ver")
	}
	c.SetNoEvict(true)
	c.SetNoTouch(true)
	if !c.GetNoEvict() || !c.GetNoTouch() {
		t.Fatal("noevict/notouch")
	}

	if c.GetProtocolVersion() != 2 {
		t.Fatalf("default proto=%d", c.GetProtocolVersion())
	}
	c.SetProtocolVersion(3)
	if c.GetProtocolVersion() != 3 {
		t.Fatal(c.GetProtocolVersion())
	}

	c.SetPassword("secret")
	if c.GetPassword() != "secret" {
		t.Fatal(c.GetPassword())
	}
	c.SetACLUser("alice")
	c.SetACLAuthenticated(true)
	if c.GetACLUser() != "alice" || !c.IsACLAuthenticated() {
		t.Fatal("acl")
	}

	c.SelectDB(3)
	if c.GetDBIndex() != 3 {
		t.Fatal(c.GetDBIndex())
	}

	c.SetSlave()
	c.SetMaster()
	if !c.IsSlave() || !c.IsMaster() {
		t.Fatal("slave/master flags")
	}
	c.SetClusterReadOnly(true)
	if !c.IsClusterReadOnly() {
		t.Fatal("readonly")
	}
	c.SetClusterReadOnly(false)
	if c.IsClusterReadOnly() {
		t.Fatal("readonly clear")
	}
	c.SetAsking(true)
	if !c.IsAsking() {
		t.Fatal("asking")
	}
	if !c.ConsumeAsking() || c.IsAsking() {
		t.Fatal("ConsumeAsking")
	}
	if c.ConsumeAsking() {
		t.Fatal("ConsumeAsking empty")
	}
}

func TestConnectionReplyMode(t *testing.T) {
	c := NewFakeConn()
	if c.GetReplyMode() != "on" || c.ShouldSuppressReply() {
		t.Fatal("default on")
	}
	c.SetReplyMode("OFF")
	if c.GetReplyMode() != "off" || !c.ShouldSuppressReply() {
		t.Fatal("off")
	}
	c.SetReplyMode("SKIP")
	if c.GetReplyMode() != "skip" {
		t.Fatal(c.GetReplyMode())
	}
	if !c.ShouldSuppressReply() {
		t.Fatal("skip once")
	}
	if c.GetReplyMode() != "on" || c.ShouldSuppressReply() {
		t.Fatal("skip consumed")
	}
	c.SetReplyMode("ON")
	if c.GetReplyMode() != "on" {
		t.Fatal(c.GetReplyMode())
	}
}

func TestConnectionSubscriptions(t *testing.T) {
	c := NewFakeConn()
	c.UnSubscribe("missing")
	c.PUnSubscribe("missing")
	c.SUnSubscribe("missing")
	if c.SubsCount() != 0 {
		t.Fatal(c.SubsCount())
	}

	c.Subscribe("ch1")
	c.Subscribe("ch2")
	c.PSubscribe("news.*")
	c.SSubscribe("shard")
	if c.SubsCount() != 4 {
		t.Fatalf("SubsCount=%d", c.SubsCount())
	}
	if c.PSubsCount() != 1 || c.SSubsCount() != 1 {
		t.Fatalf("psubs=%d ssubs=%d", c.PSubsCount(), c.SSubsCount())
	}
	chs := c.GetChannels()
	if len(chs) != 2 {
		t.Fatalf("channels=%v", chs)
	}
	pats := c.GetPatterns()
	if len(pats) != 1 || pats[0] != "news.*" {
		t.Fatalf("patterns=%v", pats)
	}

	c.UnSubscribe("ch1")
	c.PUnSubscribe("news.*")
	c.SUnSubscribe("shard")
	if c.SubsCount() != 1 {
		t.Fatalf("after unsub SubsCount=%d", c.SubsCount())
	}
}

func TestConnectionMultiAndWatch(t *testing.T) {
	c := NewFakeConn()
	if c.InMultiState() {
		t.Fatal("unexpected multi")
	}
	c.SetMultiState(true)
	if !c.InMultiState() {
		t.Fatal("multi")
	}
	c.EnqueueCmd([][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	if len(c.GetQueuedCmdLine()) != 1 {
		t.Fatal("queue")
	}
	c.AddTxError(errors.New("syntax"))
	if len(c.GetTxErrors()) != 1 {
		t.Fatal("tx errors")
	}
	c.GetWatching()["k"] = 1
	c.ClearQueuedCmds()
	if c.GetQueuedCmdLine() != nil {
		t.Fatal("clear")
	}
	c.SetMultiState(false)
	if c.InMultiState() || c.GetQueuedCmdLine() != nil {
		t.Fatal("reset multi")
	}
	if len(c.GetWatching()) != 0 {
		t.Fatalf("watching=%v", c.GetWatching())
	}
}

func TestConnectionAgeIdleAndLocalAddr(t *testing.T) {
	c := NewFakeConn()
	created := time.Now().Add(-5 * time.Second)
	active := time.Now().Add(-2 * time.Second)
	c.SetClientTimesForTest(created, active)
	if c.AgeSeconds() < 4 {
		t.Fatalf("AgeSeconds=%d", c.AgeSeconds())
	}
	if c.IdleSeconds() < 1 {
		t.Fatalf("IdleSeconds=%d", c.IdleSeconds())
	}
	c.SetClientTimesForTest(time.Time{}, time.Time{})
	if c.AgeSeconds() != 0 || c.IdleSeconds() != 0 {
		t.Fatal("zero clocks")
	}
	c.SetLocalAddr("127.0.0.1:7000")
	if c.LocalAddr() != "127.0.0.1:7000" {
		t.Fatal(c.LocalAddr())
	}
}

func TestNewConnWriteClose(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	done := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 16)
		n, _ := client.Read(buf)
		done <- buf[:n]
	}()

	c := NewConn(server)
	if c.RemoteAddr() == "" || c.LocalAddr() == "" {
		t.Fatalf("addrs remote=%q local=%q", c.RemoteAddr(), c.LocalAddr())
	}
	if c.GetClientID() == 0 {
		t.Fatal("client id")
	}
	n, err := c.Write(nil)
	if err != nil || n != 0 {
		t.Fatalf("empty write n=%d err=%v", n, err)
	}
	if _, err := c.Write([]byte("OK")); err != nil {
		t.Fatal(err)
	}
	got := <-done
	if string(got) != "OK" {
		t.Fatalf("got %q", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
