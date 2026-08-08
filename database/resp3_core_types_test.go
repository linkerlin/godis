package database

import (
	"bytes"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestResp3CoreWireTypes verifies HELLO-3 wire forms for HGETALL / SMEMBERS / ZSCORE / ZMSCORE
// while RESP2 path stays array/bulk (Map/Set/Double ToBytes downgrade).
func TestResp3CoreWireTypes(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "x", "y"))
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "m", "2", "n"))

	// --- RESP2 wire (ToBytes) ---
	h := db.Exec(nil, utils.ToCmdLine("HGETALL", "h"))
	if _, ok := h.(*protocol.MapReply); !ok {
		t.Fatalf("HGETALL type %T", h)
	}
	if h.ToBytes()[0] != '*' {
		t.Fatalf("HGETALL RESP2 should be array: %q", h.ToBytes())
	}
	if !bytes.Contains(h.ToBytes(), []byte("$1\r\na\r\n")) {
		t.Fatalf("HGETALL RESP2 missing field a: %q", h.ToBytes())
	}

	s := db.Exec(nil, utils.ToCmdLine("SMEMBERS", "s"))
	if _, ok := s.(*protocol.SetReply); !ok {
		t.Fatalf("SMEMBERS type %T", s)
	}
	if s.ToBytes()[0] != '*' {
		t.Fatalf("SMEMBERS RESP2 should be array: %q", s.ToBytes())
	}

	zs := db.Exec(nil, utils.ToCmdLine("ZSCORE", "z", "m"))
	if _, ok := zs.(*protocol.DoubleReply); !ok {
		t.Fatalf("ZSCORE type %T", zs)
	}
	asserts.AssertBulkReply(t, zs, "1.5")
	if protocol.ReplyToRESP3(zs)[0] != ',' {
		t.Fatalf("ZSCORE RESP3 should be double: %q", protocol.ReplyToRESP3(zs))
	}

	// --- RESP3 wire (ReplyToRESP3) ---
	if got := protocol.ReplyToRESP3(h); got[0] != '%' {
		t.Fatalf("HGETALL RESP3 should be map: %q", got)
	}
	if got := protocol.ReplyToRESP3(s); got[0] != '~' {
		t.Fatalf("SMEMBERS RESP3 should be set: %q", got)
	}

	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "z", "m", "missing"))
	mr, ok := zm.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("ZMSCORE type %T %s", zm, zm.ToBytes())
	}
	if _, ok := mr.Replies[0].(*protocol.DoubleReply); !ok {
		t.Fatalf("ZMSCORE[0] want Double, got %T", mr.Replies[0])
	}
	if _, ok := mr.Replies[1].(*protocol.NullBulkReply); !ok {
		t.Fatalf("ZMSCORE[1] want NullBulk, got %T", mr.Replies[1])
	}
	got := protocol.ReplyToRESP3(zm)
	if !bytes.Contains(got, []byte(",1.5\r\n")) || !bytes.Contains(got, []byte("_\r\n")) {
		t.Fatalf("ZMSCORE RESP3: %q", got)
	}

	// Empty forms
	emptyH := db.Exec(nil, utils.ToCmdLine("HGETALL", "nohash"))
	if protocol.ReplyToRESP3(emptyH)[0] != '%' {
		t.Fatalf("empty HGETALL RESP3: %q", protocol.ReplyToRESP3(emptyH))
	}
	emptyS := db.Exec(nil, utils.ToCmdLine("SMEMBERS", "noset"))
	if protocol.ReplyToRESP3(emptyS)[0] != '~' {
		t.Fatalf("empty SMEMBERS RESP3: %q", protocol.ReplyToRESP3(emptyS))
	}

	// Server path with HELLO 3 uses ReplyToRESP3; smoke via connection protocol version field.
	c := connection.NewConn(nil)
	c.SetProtocolVersion(3)
	if c.GetProtocolVersion() != 3 {
		t.Fatal("protocol version")
	}
}
