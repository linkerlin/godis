package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
)

func TestConstReplies(t *testing.T) {
	cases := []struct {
		name string
		got  []byte
		want string
	}{
		{"pong", (&PongReply{}).ToBytes(), "+PONG\r\n"},
		{"ok", MakeOkReply().ToBytes(), "+OK\r\n"},
		{"null bulk", MakeNullBulkReply().ToBytes(), "$-1\r\n"},
		{"null multi", MakeNullMultiBulkReply().ToBytes(), "*-1\r\n"},
		{"empty multi", MakeEmptyMultiBulkReply().ToBytes(), "*0\r\n"},
		{"no reply", (&NoReply{}).ToBytes(), ""},
		{"queued", MakeQueuedReply().ToBytes(), "+QUEUED\r\n"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Fatalf("%s: got %q want %q", tc.name, tc.got, tc.want)
		}
	}
	if !IsEmptyMultiBulkReply(MakeEmptyMultiBulkReply()) {
		t.Fatal("IsEmptyMultiBulkReply")
	}
	if IsEmptyMultiBulkReply(MakeIntReply(1)) {
		t.Fatal("non-empty should be false")
	}
}

func TestErrorReplies(t *testing.T) {
	unknown := &UnknownErrReply{}
	if unknown.Error() != "ERR unknown error" || !bytes.Equal(unknown.ToBytes(), []byte("-ERR unknown error\r\n")) {
		t.Fatal(unknown)
	}
	arg := MakeArgNumErrReply("get")
	if !strings.Contains(arg.Error(), "get") || !bytes.HasPrefix(arg.ToBytes(), []byte("-ERR")) {
		t.Fatal(string(arg.ToBytes()))
	}
	syn := MakeSyntaxErrReply()
	if syn.Error() != "ERR syntax error" {
		t.Fatal(syn.Error())
	}
	wt := &WrongTypeErrReply{}
	if !strings.HasPrefix(wt.Error(), "WRONGTYPE") {
		t.Fatal(wt.Error())
	}
	pe := &ProtocolErrReply{Msg: "bad"}
	if !strings.Contains(pe.Error(), "bad") || !bytes.Contains(pe.ToBytes(), []byte("bad")) {
		t.Fatal(pe)
	}
}

func TestClusterRedirectReplies(t *testing.T) {
	moved := MakeMovedErrReply(3999, "127.0.0.1:7001")
	if moved.Error() != "MOVED 3999 127.0.0.1:7001" {
		t.Fatal(moved.Error())
	}
	if string(moved.ToBytes()) != "-MOVED 3999 127.0.0.1:7001\r\n" {
		t.Fatal(string(moved.ToBytes()))
	}
	ask := MakeAskErrReply(10, "127.0.0.1:7002")
	if ask.Error() != "ASK 10 127.0.0.1:7002" {
		t.Fatal(ask.Error())
	}
	if string(ask.ToBytes()) != "-ASK 10 127.0.0.1:7002\r\n" {
		t.Fatal(string(ask.ToBytes()))
	}
}

func TestReplyHelpers(t *testing.T) {
	if !IsOKReply(MakeOkReply()) {
		t.Fatal("IsOKReply")
	}
	if IsOKReply(MakeStatusReply("PONG")) {
		t.Fatal("PONG is not OK")
	}
	if IsErrorReply(nil) || IsErrorReply(MakeOkReply()) {
		t.Fatal("non-error")
	}
	errReply := MakeErrReply("ERR boom")
	if !IsErrorReply(errReply) || errReply.Error() != "ERR boom" {
		t.Fatal(errReply)
	}
	if err := Try2ErrorReply(errReply); err == nil || !strings.Contains(err.Error(), "ERR boom") {
		t.Fatalf("Try2ErrorReply err=%v", err)
	}
	if err := Try2ErrorReply(MakeOkReply()); err != nil {
		t.Fatal(err)
	}
	if err := Try2ErrorReply(&NoReply{}); err == nil {
		t.Fatal("empty reply should error")
	}

	bulkNil := MakeBulkReply(nil)
	if !bytes.Equal(bulkNil.ToBytes(), []byte("$-1\r\n")) {
		t.Fatal(string(bulkNil.ToBytes()))
	}
	st := MakeStatusReply("OK")
	if string(st.ToBytes()) != "+OK\r\n" {
		t.Fatal(string(st.ToBytes()))
	}
	mr := MakeMultiRawReply([]redis.Reply{MakeIntReply(1), MakeBulkReply([]byte("a"))})
	if string(mr.ToBytes()) != "*2\r\n:1\r\n$1\r\na\r\n" {
		t.Fatal(string(mr.ToBytes()))
	}
}

func TestPushMessageBuilders(t *testing.T) {
	inv := MakeInvalidatePush([]string{"k1", "k2"})
	raw := inv.ToBytes()
	if raw[0] != '>' || !bytes.Contains(raw, []byte("invalidate")) {
		t.Fatalf("%q", raw)
	}
	msg := MakeMessagePush("ch", []byte("hi"))
	if msg.Kind != "message" || len(msg.Data) != 2 {
		t.Fatal(msg)
	}
	sub := MakeSubscribePush("ch", 1)
	unsub := MakeUnsubscribePush("ch", 0)
	smsg := MakeSMessagePush("ch", []byte("x"))
	for _, p := range []*PushMessage{sub, unsub, smsg} {
		b := p.ToBytes()
		if b[0] != '>' {
			t.Fatalf("%s -> %q", p.Kind, b)
		}
	}

	parsed, err := ParsePush(inv.ToBytes())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Kind != "invalidate" {
		t.Fatalf("kind=%q", parsed.Kind)
	}
	if _, err := ParsePush([]byte("+OK\r\n")); err == nil {
		t.Fatal("expected non-push error")
	}
	if _, err := ParsePush([]byte(">")); err == nil {
		t.Fatal("expected invalid format")
	}
}

func TestRESP3ParserRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		in    string
		check func(t *testing.T, r redis.Reply)
	}{
		{"simple", "+OK\r\n", func(t *testing.T, r redis.Reply) {
			sr, ok := r.(*StatusReply)
			if !ok || sr.Status != "OK" {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"error", "-ERR x\r\n", func(t *testing.T, r redis.Reply) {
			er, ok := r.(*StandardErrReply)
			if !ok || er.Status != "ERR x" {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"int", ":42\r\n", func(t *testing.T, r redis.Reply) {
			ir, ok := r.(*IntReply)
			if !ok || ir.Code != 42 {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"null", "_\r\n", func(t *testing.T, r redis.Reply) {
			if _, ok := r.(*NullReply); !ok {
				t.Fatalf("%T", r)
			}
		}},
		{"double", ",3.5\r\n", func(t *testing.T, r redis.Reply) {
			dr, ok := r.(*DoubleReply)
			if !ok || dr.Value != 3.5 {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"bool", "#t\r\n", func(t *testing.T, r redis.Reply) {
			br, ok := r.(*BooleanReply)
			if !ok || !br.Value {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"bignum", "(12345678901234567890\r\n", func(t *testing.T, r redis.Reply) {
			bn, ok := r.(*BigNumberReply)
			if !ok || bn.Value != "12345678901234567890" {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"bulk", "$3\r\nfoo\r\n", func(t *testing.T, r redis.Reply) {
			br, ok := r.(*BulkReply)
			if !ok || string(br.Arg) != "foo" {
				t.Fatalf("%T %v", r, r)
			}
		}},
		{"verbatim", "=9\r\ntxt:hello\r\n", func(t *testing.T, r redis.Reply) {
			vr, ok := r.(*VerbatimReply)
			if !ok || vr.Format != "txt" || vr.Value != "hello" {
				t.Fatalf("%T %+v", r, r)
			}
		}},
		{"array", "*2\r\n$1\r\nx\r\n$1\r\ny\r\n", func(t *testing.T, r redis.Reply) {
			ar, ok := r.(*MultiBulkReply)
			if !ok || len(ar.Args) != 2 {
				t.Fatalf("%T %+v", r, r)
			}
		}},
		{"map", "%1\r\n$1\r\nk\r\n$1\r\nv\r\n", func(t *testing.T, r redis.Reply) {
			mr, ok := r.(*MapReply)
			if !ok || len(mr.Data) != 1 {
				t.Fatalf("%T %+v", r, r)
			}
		}},
		{"set", "~1\r\n$1\r\nx\r\n", func(t *testing.T, r redis.Reply) {
			sr, ok := r.(*SetReply)
			if !ok || len(sr.Data) != 1 {
				t.Fatalf("%T %+v", r, r)
			}
		}},
		{"push", ">2\r\n$7\r\nmessage\r\n$2\r\nhi\r\n", func(t *testing.T, r redis.Reply) {
			pr, ok := r.(*PushReply)
			if !ok || pr.Kind != "message" {
				t.Fatalf("%T %+v", r, r)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRESP3Parser([]byte(tc.in)).Parse()
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, r)
		})
	}
	if _, err := NewRESP3Parser([]byte("?\r\n")).Parse(); err == nil {
		t.Fatal("unknown type")
	}
}
