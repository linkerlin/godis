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
	db.Flush()
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

func TestResp3ScorePairsWireTypes(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "a", "2", "b", "3", "c"))

	zr := db.Exec(nil, utils.ToCmdLine("ZRANGE", "z", "0", "-1", "WITHSCORES"))
	sp, ok := zr.(*protocol.ScorePairsReply)
	if !ok || !sp.Nest {
		t.Fatalf("ZRANGE WITHSCORES type %T nest=%v", zr, ok && sp.Nest)
	}
	if zr.ToBytes()[0] != '*' || !bytes.Contains(zr.ToBytes(), []byte("$3\r\n1.5\r\n")) {
		t.Fatalf("ZRANGE RESP2 flat: %q", zr.ToBytes())
	}
	got := protocol.ReplyToRESP3(zr)
	if !bytes.HasPrefix(got, []byte("*3\r\n*2\r\n")) || !bytes.Contains(got, []byte(",1.5\r\n")) {
		t.Fatalf("ZRANGE RESP3 nested: %q", got)
	}

	// Bare ZPOPMIN → flat RESP3
	pop := db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "z"))
	sp2, ok := pop.(*protocol.ScorePairsReply)
	if !ok || sp2.Nest {
		t.Fatalf("ZPOPMIN bare type %T nest=%v", pop, ok && sp2.Nest)
	}
	if g := protocol.ReplyToRESP3(pop); !bytes.HasPrefix(g, []byte("*2\r\n")) || bytes.Contains(g, []byte("*2\r\n*2\r\n")) {
		t.Fatalf("bare ZPOPMIN RESP3 should be flat: %q", g)
	}

	// Explicit COUNT → nested
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z2", "1", "x", "2", "y"))
	popN := db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "z2", "2"))
	sp3, ok := popN.(*protocol.ScorePairsReply)
	if !ok || !sp3.Nest {
		t.Fatalf("ZPOPMIN count type %T nest=%v", popN, ok && sp3.Nest)
	}
	if g := protocol.ReplyToRESP3(popN); !bytes.HasPrefix(g, []byte("*2\r\n*2\r\n")) {
		t.Fatalf("ZPOPMIN COUNT RESP3 nested: %q", g)
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "zi", "0.5", "m")), "0.5")
	zi := db.Exec(nil, utils.ToCmdLine("ZINCRBY", "zi", "1", "m"))
	if _, ok := zi.(*protocol.DoubleReply); !ok {
		t.Fatalf("ZINCRBY type %T", zi)
	}
	if protocol.ReplyToRESP3(zi)[0] != ',' {
		t.Fatalf("ZINCRBY RESP3: %q", protocol.ReplyToRESP3(zi))
	}
}

func TestResp3SetOpWireTypes(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "a", "1", "2", "3"))
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "b", "2", "3", "4"))

	inter := db.Exec(nil, utils.ToCmdLine("SINTER", "a", "b"))
	if _, ok := inter.(*protocol.SetReply); !ok {
		t.Fatalf("SINTER type %T", inter)
	}
	if inter.ToBytes()[0] != '*' {
		t.Fatalf("SINTER RESP2: %q", inter.ToBytes())
	}
	if protocol.ReplyToRESP3(inter)[0] != '~' {
		t.Fatalf("SINTER RESP3: %q", protocol.ReplyToRESP3(inter))
	}
	asserts.AssertMultiBulkReplySize(t, inter, 2)

	uni := db.Exec(nil, utils.ToCmdLine("SUNION", "a", "b"))
	if protocol.ReplyToRESP3(uni)[0] != '~' {
		t.Fatalf("SUNION RESP3: %q", protocol.ReplyToRESP3(uni))
	}
	asserts.AssertMultiBulkReplySize(t, uni, 4)

	diff := db.Exec(nil, utils.ToCmdLine("SDIFF", "a", "b"))
	if protocol.ReplyToRESP3(diff)[0] != '~' {
		t.Fatalf("SDIFF RESP3: %q", protocol.ReplyToRESP3(diff))
	}
	asserts.AssertMultiBulkReplySize(t, diff, 1)

	empty := db.Exec(nil, utils.ToCmdLine("SINTER", "a", "nosuch"))
	if g := protocol.ReplyToRESP3(empty); string(g) != "~0\r\n" {
		t.Fatalf("empty SINTER RESP3: %q", g)
	}
}

func TestResp3ConfigGetAndHRandFieldMap(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	cfg := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "port"))
	if _, ok := cfg.(*protocol.MapReply); !ok {
		t.Fatalf("CONFIG GET type %T", cfg)
	}
	if cfg.ToBytes()[0] != '*' {
		t.Fatalf("CONFIG GET RESP2: %q", cfg.ToBytes())
	}
	if protocol.ReplyToRESP3(cfg)[0] != '%' {
		t.Fatalf("CONFIG GET RESP3: %q", protocol.ReplyToRESP3(cfg))
	}
	if _, ok := configReplyValue(cfg, "port"); !ok {
		t.Fatalf("CONFIG GET missing port: %s", cfg.ToBytes())
	}

	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))
	pos := db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "h", "2", "WITHVALUES"))
	if _, ok := pos.(*protocol.MapReply); !ok {
		t.Fatalf("HRANDFIELD + WITHVALUES type %T", pos)
	}
	if protocol.ReplyToRESP3(pos)[0] != '%' {
		t.Fatalf("HRANDFIELD WITHVALUES RESP3: %q", protocol.ReplyToRESP3(pos))
	}
	// Negative count stays array (duplicates allowed).
	neg := db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "h", "-3", "WITHVALUES"))
	if _, ok := neg.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("HRANDFIELD negative WITHVALUES should stay MultiBulk, got %T", neg)
	}
	if protocol.ReplyToRESP3(neg)[0] != '*' {
		t.Fatalf("negative WITHVALUES RESP3 should be array: %q", protocol.ReplyToRESP3(neg))
	}
}

func TestResp3SPopSRandMemberSet(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c", "d"))

	sr := db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "s", "2"))
	if _, ok := sr.(*protocol.SetReply); !ok {
		t.Fatalf("SRANDMEMBER +count type %T", sr)
	}
	if protocol.ReplyToRESP3(sr)[0] != '~' {
		t.Fatalf("SRANDMEMBER +count RESP3: %q", protocol.ReplyToRESP3(sr))
	}
	neg := db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "s", "-3"))
	if _, ok := neg.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("SRANDMEMBER -count should stay MultiBulk, got %T", neg)
	}

	pop := db.Exec(nil, utils.ToCmdLine("SPOP", "s", "2"))
	if _, ok := pop.(*protocol.SetReply); !ok {
		t.Fatalf("SPOP count type %T", pop)
	}
	if protocol.ReplyToRESP3(pop)[0] != '~' {
		t.Fatalf("SPOP count RESP3: %q", protocol.ReplyToRESP3(pop))
	}
	empty := db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "nosuch", "3"))
	if g := protocol.ReplyToRESP3(empty); string(g) != "~0\r\n" {
		t.Fatalf("empty SRANDMEMBER RESP3: %q", g)
	}
}
