package protocol

import (
	"bytes"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
)

func TestReplyToRESP3Nulls(t *testing.T) {
	cases := []struct {
		name     string
		reply    redis.Reply
		expected []byte
	}{
		{"nil reply", nil, []byte("_\r\n")},
		{"NullBulkReply", MakeNullBulkReply(), []byte("_\r\n")},
		{"NullReply", &NullReply{}, []byte("_\r\n")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ReplyToRESP3(tc.reply)
			if !bytes.Equal(got, tc.expected) {
				t.Fatalf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestReplyToRESP3MultiBulkWithNulls(t *testing.T) {
	reply := MakeMultiBulkReply([][]byte{[]byte("a"), nil, []byte("b")})
	got := ReplyToRESP3(reply)
	expected := []byte("*3\r\n$1\r\na\r\n_\r\n$1\r\nb\r\n")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestReplyToRESP3MultiRaw(t *testing.T) {
	reply := MakeMultiRawReply([]redis.Reply{
		MakeIntReply(1),
		MakeDoubleReply(2.5),
		&NullReply{},
	})
	got := ReplyToRESP3(reply)
	expected := []byte("*3\r\n:1\r\n,2.5\r\n_\r\n")
	if !bytes.Equal(got, expected) {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestRESP3RepliesEncode(t *testing.T) {
	cases := []struct {
		name     string
		reply    redis.Reply
		expected []byte
	}{
		{"bool true", MakeBooleanReply(true), []byte("#t\r\n")},
		{"bool false", MakeBooleanReply(false), []byte("#f\r\n")},
		{"double", MakeDoubleReply(3.14), []byte(",3.14\r\n")},
		{"double integer", MakeDoubleReply(3), []byte(",3\r\n")},
		{"bignum", MakeBigNumberReply("999999999999999999999"), []byte("(999999999999999999999\r\n")},
		{"verbatim", MakeVerbatimReply("txt", "hello"), []byte("=9\r\ntxt:hello\r\n")},
		{"set", MakeSetReply([]redis.Reply{MakeBulkReply([]byte("x")), MakeIntReply(7)}), []byte("~2\r\n$1\r\nx\r\n:7\r\n")},
		{"push", MakePushReply("invalidate", []redis.Reply{MakeBulkReply([]byte("k"))}), []byte(">2\r\n$10\r\ninvalidate\r\n$1\r\nk\r\n")},
		{"map", func() redis.Reply {
			m := MakeMapReply()
			m.Put("k", MakeBulkReply([]byte("v")))
			return m
		}(), []byte("%1\r\n$1\r\nk\r\n$1\r\nv\r\n")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.reply.ToBytes()
			if !bytes.Equal(got, tc.expected) {
				t.Fatalf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestAttributeReplyEncoding(t *testing.T) {
	attrs := MakeMapReply()
	attrs.Put("popularity", MakeBulkReply([]byte("high")))
	attr := MakeAttributeReply(attrs, MakeIntReply(42))
	got := attr.ToBytes()
	if !bytes.HasPrefix(got, []byte("|1\r\n")) {
		t.Fatalf("expected attribute prefix, got %q", got)
	}
	if !bytes.HasSuffix(got, []byte(":42\r\n")) {
		t.Fatalf("expected trailing reply, got %q", got)
	}
}
