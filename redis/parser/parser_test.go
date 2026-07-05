package parser

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestParseStream(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}
	reqs := bytes.Buffer{}
	for _, re := range replies {
		reqs.Write(re.ToBytes())
	}
	reqs.Write([]byte("set a a" + protocol.CRLF)) // test text protocol
	expected := make([]redis.Reply, len(replies))
	copy(expected, replies)
	expected = append(expected, protocol.MakeMultiBulkReply([][]byte{
		[]byte("set"), []byte("a"), []byte("a"),
	}))

	ch := ParseStream(bytes.NewReader(reqs.Bytes()))
	i := 0
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF {
				return
			}
			t.Error(payload.Err)
			return
		}
		if payload.Data == nil {
			t.Error("empty data")
			return
		}
		exp := expected[i]
		i++
		if !utils.BytesEquals(exp.ToBytes(), payload.Data.ToBytes()) {
			t.Error("parse failed: " + string(exp.ToBytes()))
		}
	}
}

func TestParseOne(t *testing.T) {
	replies := []redis.Reply{
		protocol.MakeIntReply(1),
		protocol.MakeStatusReply("OK"),
		protocol.MakeErrReply("ERR unknown"),
		protocol.MakeBulkReply([]byte("a\r\nb")), // test binary safe
		protocol.MakeNullBulkReply(),
		protocol.MakeMultiBulkReply([][]byte{
			[]byte("a"),
			[]byte("\r\n"),
		}),
		protocol.MakeEmptyMultiBulkReply(),
	}
	for _, re := range replies {
		result, err := ParseOne(re.ToBytes())
		if err != nil {
			t.Error(err)
			continue
		}
		if !utils.BytesEquals(result.ToBytes(), re.ToBytes()) {
			t.Error("parse failed: " + string(re.ToBytes()))
		}
	}
}

func TestParseRejectsHugeArrayHeader(t *testing.T) {
	data := []byte("*155555555\r\n\n0\r\n")
	done := make(chan struct{})
	var parseErr error
	go func() {
		_, parseErr = ParseOne(data)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ParseOne hung on huge array header")
	}
	if parseErr == nil {
		t.Fatal("expected protocol error for huge array")
	}
}

func TestParseRejectsHugeBulkHeader(t *testing.T) {
	data := []byte("$999999999999\r\n")
	_, err := ParseOne(data)
	if err == nil {
		t.Fatal("expected error for huge bulk header")
	}
}

func TestParseRejectsHugeBulkInArray(t *testing.T) {
	data := []byte("*1\r\n$42222222222\r\nPING\r\n")
	done := make(chan struct{})
	var parseErr error
	go func() {
		_, parseErr = ParseOne(data)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ParseOne hung on huge bulk in array")
	}
	if parseErr == nil {
		t.Fatal("expected protocol error for huge bulk in array")
	}
}

func TestParseRESP3Types(t *testing.T) {
	cases := []struct {
		name  string
		input redis.Reply
	}{
		{"null", &protocol.NullReply{}},
		{"bool-true", protocol.MakeBooleanReply(true)},
		{"bool-false", protocol.MakeBooleanReply(false)},
		{"double-int", protocol.MakeDoubleReply(3)},
		{"double-frac", protocol.MakeDoubleReply(3.14)},
		{"bignum", protocol.MakeBigNumberReply("12345678901234567890")},
		{"verbatim", protocol.MakeVerbatimReply("txt", "hello")},
		{"set", protocol.MakeSetReply([]redis.Reply{
			protocol.MakeBulkReply([]byte("a")),
			protocol.MakeIntReply(1),
		})},
		{"push", protocol.MakePushReply("invalidate", []redis.Reply{
			protocol.MakeMultiBulkReply([][]byte{[]byte("key1")}),
		})},
		{"multi-raw", protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(1),
			protocol.MakeDoubleReply(2.5),
			&protocol.NullReply{},
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			original := tc.input.ToBytes()
			parsed, err := ParseOne(original)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if !utils.BytesEquals(parsed.ToBytes(), original) {
				t.Fatalf("roundtrip mismatch:\noriginal: %q\nparsed:   %q", original, parsed.ToBytes())
			}
		})
	}
}

func TestParseRESP3Map(t *testing.T) {
	m := protocol.MakeMapReply()
	m.Put("proto", protocol.MakeIntReply(3))
	m.Put("server", protocol.MakeBulkReply([]byte("godis")))
	parsed, err := ParseOne(m.ToBytes())
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	parsedMap, ok := parsed.(*protocol.MapReply)
	if !ok {
		t.Fatalf("expected MapReply, got %T", parsed)
	}
	if len(parsedMap.Data) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(parsedMap.Data))
	}
	proto, exists := parsedMap.Data["proto"]
	if !exists {
		t.Fatalf("missing 'proto' key")
	}
	if ir, ok := proto.(*protocol.IntReply); !ok || ir.Code != 3 {
		t.Fatalf("expected proto=3, got %v", proto)
	}
}

func TestParseRESP3Attribute(t *testing.T) {
	attrs := protocol.MakeMapReply()
	attrs.Put("key-popularity", protocol.MakeBulkReply([]byte("high")))
	attr := protocol.MakeAttributeReply(attrs, protocol.MakeIntReply(42))
	parsed, err := ParseOne(attr.ToBytes())
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	parsedAttr, ok := parsed.(*protocol.AttributeReply)
	if !ok {
		t.Fatalf("expected AttributeReply, got %T", parsed)
	}
	if len(parsedAttr.Attributes.Data) != 1 {
		t.Fatalf("expected 1 attribute, got %d", len(parsedAttr.Attributes.Data))
	}
	if ir, ok := parsedAttr.Reply.(*protocol.IntReply); !ok || ir.Code != 42 {
		t.Fatalf("expected reply 42, got %v", parsedAttr.Reply)
	}
}

func TestParseRESP3ArrayContainsNull(t *testing.T) {
	arr := protocol.MakeMultiBulkReply([][]byte{[]byte("a"), nil, []byte("b")})
	parsed, err := ParseOne(arr.ToBytes())
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	multi, ok := parsed.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected MultiBulkReply, got %T", parsed)
	}
	if len(multi.Args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(multi.Args))
	}
	if multi.Args[0] == nil || string(multi.Args[0]) != "a" {
		t.Fatalf("expected a, got %v", multi.Args[0])
	}
	if multi.Args[1] != nil {
		t.Fatalf("expected nil, got %v", multi.Args[1])
	}
}
