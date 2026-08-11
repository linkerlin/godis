package pubsub

import (
	"bytes"
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestHubNumSubAndChannels(t *testing.T) {
	hub := MakeHub()
	c1 := connection.NewFakeConn()
	c2 := connection.NewFakeConn()

	if hub.NumSub("ch") != 0 || hub.NumChannels() != 0 || hub.NumPat() != 0 {
		t.Fatal("empty hub")
	}

	Subscribe(hub, c1, [][]byte{[]byte("news"), []byte("sports")})
	Subscribe(hub, c2, [][]byte{[]byte("news")})
	if hub.NumSub("news") != 2 || hub.NumSub("sports") != 1 {
		t.Fatalf("numsub news=%d sports=%d", hub.NumSub("news"), hub.NumSub("sports"))
	}
	if hub.NumChannels() != 2 {
		t.Fatalf("NumChannels=%d", hub.NumChannels())
	}

	seen := map[string]int{}
	hub.ForEachChannel(func(channel string, n int) bool {
		seen[channel] = n
		return true
	})
	if seen["news"] != 2 || seen["sports"] != 1 {
		t.Fatalf("ForEachChannel=%v", seen)
	}
}

func TestSubscribePublishUnsubscribe(t *testing.T) {
	hub := MakeHub()
	sub := connection.NewFakeConn()
	Subscribe(hub, sub, [][]byte{[]byte("ch")})
	if !bytes.Contains(sub.Bytes(), []byte("subscribe")) {
		t.Fatalf("missing subscribe push: %q", sub.Bytes())
	}
	sub.Clean()

	pub := Publish(hub, [][]byte{[]byte("ch"), []byte("hello")})
	ir, ok := pub.(*protocol.IntReply)
	if !ok || ir.Code != 1 {
		t.Fatalf("publish reply=%v", pub)
	}
	if !bytes.Contains(sub.Bytes(), []byte("hello")) {
		t.Fatalf("subscriber missed message: %q", sub.Bytes())
	}
	sub.Clean()

	UnSubscribe(hub, sub, [][]byte{[]byte("ch")})
	if hub.NumSub("ch") != 0 {
		t.Fatalf("still subscribed: %d", hub.NumSub("ch"))
	}
	if !bytes.Contains(sub.Bytes(), []byte("unsubscribe")) {
		t.Fatalf("missing unsubscribe: %q", sub.Bytes())
	}

	idle := connection.NewFakeConn()
	UnSubscribe(hub, idle, nil)
	if !bytes.Contains(idle.Bytes(), []byte("unsubscribe")) {
		t.Fatalf("empty unsub: %q", idle.Bytes())
	}
}

func TestPublishPatternMatch(t *testing.T) {
	hub := MakeHub()
	sub := connection.NewFakeConn()
	if reply := PSubscribe(hub, sub, nil); reply.ToBytes()[0] != '-' {
		t.Fatalf("expected arity err, got %q", reply.ToBytes())
	}
	PSubscribe(hub, sub, [][]byte{[]byte("news.*")})
	if hub.NumPat() != 1 {
		t.Fatalf("NumPat=%d", hub.NumPat())
	}
	sub.Clean()

	pub := Publish(hub, [][]byte{[]byte("news.sports"), []byte("goal")})
	ir := pub.(*protocol.IntReply)
	if ir.Code != 1 {
		t.Fatalf("receivers=%d", ir.Code)
	}
	got := string(sub.Bytes())
	if !strings.Contains(got, "pmessage") || !strings.Contains(got, "goal") {
		t.Fatalf("pmessage missing: %q", got)
	}

	sub.Clean()
	PUnSubscribe(hub, sub, [][]byte{[]byte("news.*")})
	if hub.NumPat() != 0 {
		t.Fatalf("NumPat after unsub=%d", hub.NumPat())
	}
	idle := connection.NewFakeConn()
	PUnSubscribe(hub, idle, nil)
	if !bytes.Contains(idle.Bytes(), []byte("punsubscribe")) {
		t.Fatalf("empty punsub: %q", idle.Bytes())
	}
}

func TestPublishArgCountAndRESP3(t *testing.T) {
	hub := MakeHub()
	if reply := Publish(hub, [][]byte{[]byte("only")}); reply.ToBytes()[0] != '-' {
		t.Fatalf("expected arity err: %q", reply.ToBytes())
	}

	sub := connection.NewFakeConn()
	sub.SetProtocolVersion(3)
	Subscribe(hub, sub, [][]byte{[]byte("ch")})
	if !bytes.HasPrefix(sub.Bytes(), []byte(">")) {
		t.Fatalf("expected RESP3 push, got %q", sub.Bytes())
	}
}

func TestUnsubscribeAll(t *testing.T) {
	hub := MakeHub()
	c := connection.NewFakeConn()
	Subscribe(hub, c, [][]byte{[]byte("a"), []byte("b")})
	PSubscribe(hub, c, [][]byte{[]byte("p*")})
	UnsubscribeAll(hub, c)
	if hub.NumChannels() != 0 || hub.NumPat() != 0 || c.SubsCount() != 0 {
		t.Fatalf("channels=%d pat=%d subs=%d", hub.NumChannels(), hub.NumPat(), c.SubsCount())
	}
}

func TestShardedHubLifecycle(t *testing.T) {
	sh := NewShardedHub()
	c := connection.NewFakeConn()

	if reply := sh.Subscribe(nil, []string{"x"}); !protocol.IsErrorReply(reply) {
		t.Fatalf("nil subscribe: %v", reply)
	}
	sh.Subscribe(c, []string{"foo", "bar"})
	if sh.NumSub("foo") != 1 {
		t.Fatalf("NumSub=%d", sh.NumSub("foo"))
	}
	chs := sh.Channels()
	if len(chs) != 2 {
		t.Fatalf("Channels=%v", chs)
	}
	if sh.GetSlot("foo") != 12182 {
		t.Fatalf("GetSlot=%d", sh.GetSlot("foo"))
	}
	if !bytes.Contains(c.Bytes(), []byte("ssubscribe")) {
		t.Fatalf("ssubscribe push missing: %q", c.Bytes())
	}
	c.Clean()

	if n := sh.Publish("foo", []byte("hi")); n != 1 {
		t.Fatalf("Publish n=%d", n)
	}
	if !bytes.Contains(c.Bytes(), []byte("smessage")) || !bytes.Contains(c.Bytes(), []byte("hi")) {
		t.Fatalf("smessage: %q", c.Bytes())
	}
	c.Clean()

	sh.Unsubscribe(c, []string{"foo"})
	if sh.NumSub("foo") != 0 || sh.NumSub("bar") != 1 {
		t.Fatalf("after unsub foo=%d bar=%d", sh.NumSub("foo"), sh.NumSub("bar"))
	}

	sh.Unsubscribe(c, nil)
	if len(sh.Channels()) != 0 {
		t.Fatalf("channels left: %v", sh.Channels())
	}

	idle := connection.NewFakeConn()
	sh.Unsubscribe(idle, nil)
	if !bytes.Contains(idle.Bytes(), []byte("sunsubscribe")) {
		t.Fatalf("empty sunsub: %q", idle.Bytes())
	}

	c2 := connection.NewFakeConn()
	sh.Subscribe(c2, []string{"z"})
	sh.AfterClientClose(c2)
	if sh.NumSub("z") != 0 {
		t.Fatal("AfterClientClose")
	}
	sh.AfterClientClose(nil)
}
