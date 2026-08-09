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

func TestResp3ZUnionMembersSet(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z1", "1", "a", "2", "b"))
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z2", "3", "b", "4", "c"))
	u := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "z1", "z2"))
	if _, ok := u.(*protocol.SetReply); !ok {
		t.Fatalf("ZUNION members type %T", u)
	}
	if protocol.ReplyToRESP3(u)[0] != '~' {
		t.Fatalf("ZUNION members RESP3: %q", protocol.ReplyToRESP3(u))
	}
	asserts.AssertMultiBulkReplySize(t, u, 3)
	// WITHSCORES stays ScorePairs (nested), not Set.
	ws := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "z1", "z2", "WITHSCORES"))
	if _, ok := ws.(*protocol.ScorePairsReply); !ok {
		t.Fatalf("ZUNION WITHSCORES type %T", ws)
	}
}

func TestResp3XInfoMap(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("XADD", "xi", "*", "f", "v"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xi", "g", "0-0")), "OK")

	info := db.Exec(nil, utils.ToCmdLine("XINFO", "STREAM", "xi"))
	if _, ok := info.(*protocol.MapReply); !ok {
		t.Fatalf("XINFO STREAM type %T", info)
	}
	if info.ToBytes()[0] != '*' {
		t.Fatalf("XINFO STREAM RESP2: %q", info.ToBytes())
	}
	if protocol.ReplyToRESP3(info)[0] != '%' {
		t.Fatalf("XINFO STREAM RESP3: %q", protocol.ReplyToRESP3(info))
	}

	groups := db.Exec(nil, utils.ToCmdLine("XINFO", "GROUPS", "xi"))
	mr, ok := groups.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("XINFO GROUPS type %T %s", groups, groups.ToBytes())
	}
	if _, ok := mr.Replies[0].(*protocol.MapReply); !ok {
		t.Fatalf("XINFO GROUPS[0] want Map, got %T", mr.Replies[0])
	}
	wire3 := protocol.ReplyToRESP3(groups)
	if wire3[0] != '*' || !bytes.Contains(wire3, []byte("%")) {
		t.Fatalf("XINFO GROUPS RESP3: %q", wire3)
	}

	_ = db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "xi", ">"))
	consumers := db.Exec(nil, utils.ToCmdLine("XINFO", "CONSUMERS", "xi", "g"))
	cmr, ok := consumers.(*protocol.MultiRawReply)
	if !ok || len(cmr.Replies) < 1 {
		t.Fatalf("XINFO CONSUMERS type %T %s", consumers, consumers.ToBytes())
	}
	if _, ok := cmr.Replies[0].(*protocol.MapReply); !ok {
		t.Fatalf("XINFO CONSUMERS[0] want Map, got %T", cmr.Replies[0])
	}
	if cw := protocol.ReplyToRESP3(consumers); cw[0] != '*' || !bytes.Contains(cw, []byte("%")) {
		t.Fatalf("XINFO CONSUMERS RESP3: %q", cw)
	}
}

func TestResp3ZRandMemberSet(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c"))

	pos := db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "z", "2"))
	if _, ok := pos.(*protocol.SetReply); !ok {
		t.Fatalf("ZRANDMEMBER +count type %T", pos)
	}
	if protocol.ReplyToRESP3(pos)[0] != '~' {
		t.Fatalf("ZRANDMEMBER +count RESP3: %q", protocol.ReplyToRESP3(pos))
	}
	asserts.AssertMultiBulkReplySize(t, pos, 2)

	neg := db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "z", "-3"))
	if _, ok := neg.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("ZRANDMEMBER -count should stay MultiBulk, got %T", neg)
	}

	ws := db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "z", "2", "WITHSCORES"))
	if _, ok := ws.(*protocol.ScorePairsReply); !ok {
		t.Fatalf("ZRANDMEMBER WITHSCORES type %T", ws)
	}
	if protocol.ReplyToRESP3(ws)[0] != '*' {
		t.Fatalf("ZRANDMEMBER WITHSCORES RESP3: %q", protocol.ReplyToRESP3(ws))
	}

	empty := db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "nosuch", "3"))
	if g := protocol.ReplyToRESP3(empty); string(g) != "~0\r\n" {
		t.Fatalf("empty ZRANDMEMBER RESP3: %q", g)
	}
}

func TestResp3MemoryStatsMap(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "ms", "v"))
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("MEMORY STATS type %T", r)
	}
	if r.ToBytes()[0] != '*' {
		t.Fatalf("MEMORY STATS RESP2: %q", r.ToBytes())
	}
	wire3 := protocol.ReplyToRESP3(r)
	if wire3[0] != '%' {
		t.Fatalf("MEMORY STATS RESP3: %q", wire3)
	}
	if _, ok := m.Data["dataset.percentage"].(*protocol.DoubleReply); !ok {
		t.Fatalf("dataset.percentage want Double, got %T", m.Data["dataset.percentage"])
	}
	if _, ok := m.Data["fragmentation"].(*protocol.DoubleReply); !ok {
		t.Fatalf("fragmentation want Double, got %T", m.Data["fragmentation"])
	}
}

func TestResp3ACLGetUserMap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"ACL", "SETUSER", "agu", "on", "nopass", "~*", "+@all",
	)), "OK")
	r := db.Exec(nil, utils.ToCmdLine("ACL", "GETUSER", "agu"))
	if _, ok := r.(*protocol.MapReply); !ok {
		t.Fatalf("ACL GETUSER type %T", r)
	}
	if r.ToBytes()[0] != '*' {
		t.Fatalf("ACL GETUSER RESP2: %q", r.ToBytes())
	}
	if protocol.ReplyToRESP3(r)[0] != '%' {
		t.Fatalf("ACL GETUSER RESP3: %q", protocol.ReplyToRESP3(r))
	}
}

func TestResp3CommandDocsAndTrackingInfoMap(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	docs := server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "get"))
	if _, ok := docs.(*protocol.MapReply); !ok {
		t.Fatalf("COMMAND DOCS type %T", docs)
	}
	if docs.ToBytes()[0] != '*' {
		t.Fatalf("COMMAND DOCS RESP2: %q", docs.ToBytes())
	}
	if protocol.ReplyToRESP3(docs)[0] != '%' {
		t.Fatalf("COMMAND DOCS RESP3: %q", protocol.ReplyToRESP3(docs))
	}
	outer := docs.(*protocol.MapReply)
	inner, ok := outer.Data["get"].(*protocol.MapReply)
	if !ok {
		t.Fatalf("COMMAND DOCS get value: %T", outer.Data["get"])
	}
	if protocol.ReplyToRESP3(inner)[0] != '%' {
		t.Fatalf("command docs value RESP3: %q", protocol.ReplyToRESP3(inner))
	}

	ti := server.Exec(c, utils.ToCmdLine("CLIENT", "TRACKINGINFO"))
	if _, ok := ti.(*protocol.MapReply); !ok {
		t.Fatalf("TRACKINGINFO type %T", ti)
	}
	if ti.ToBytes()[0] != '*' {
		t.Fatalf("TRACKINGINFO RESP2: %q", ti.ToBytes())
	}
	if protocol.ReplyToRESP3(ti)[0] != '%' {
		t.Fatalf("TRACKINGINFO RESP3: %q", protocol.ReplyToRESP3(ti))
	}
}

func TestResp3XReadMap(t *testing.T) {
	db := makeTestDB()
	db.Flush()
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", "xr", "*", "f", "v"))
	bulk, ok := idReply.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("XADD: %T %s", idReply, idReply.ToBytes())
	}
	id := string(bulk.Arg)

	read := db.Exec(nil, utils.ToCmdLine("XREAD", "COUNT", "1", "STREAMS", "xr", "0-0"))
	sr, ok := read.(*StreamReadReply)
	if !ok || len(sr.buckets) != 1 || sr.buckets[0].key != "xr" {
		t.Fatalf("XREAD type %T %s", read, read.ToBytes())
	}
	// RESP2: nested [[key, entries]]
	wire2 := read.ToBytes()
	if wire2[0] != '*' {
		t.Fatalf("XREAD RESP2 should be array: %q", wire2)
	}
	if !bytes.Contains(wire2, []byte("$2\r\nxr\r\n")) || !bytes.Contains(wire2, []byte(id)) {
		t.Fatalf("XREAD RESP2 missing key/id: %q", wire2)
	}
	// RESP3: top-level map
	wire3 := protocol.ReplyToRESP3(read)
	if wire3[0] != '%' {
		t.Fatalf("XREAD RESP3 should be map: %q", wire3)
	}
	if !bytes.Contains(wire3, []byte("$2\r\nxr\r\n")) || !bytes.Contains(wire3, []byte("%1\r\n")) {
		t.Fatalf("XREAD RESP3 missing stream/field map: %q", wire3)
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xr", "g", "0-0")), "OK")
	groupRead := db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c1", "STREAMS", "xr", ">",
	))
	if _, ok := groupRead.(*StreamReadReply); !ok {
		t.Fatalf("XREADGROUP type %T %s", groupRead, groupRead.ToBytes())
	}
	if protocol.ReplyToRESP3(groupRead)[0] != '%' {
		t.Fatalf("XREADGROUP RESP3: %q", protocol.ReplyToRESP3(groupRead))
	}
}

func TestResp3FunctionListLibraryMaps(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	code := "#!lua name=resp3flib api_version=1.0\n" +
		"redis.register_function('resp3f', function(keys, args) return 1 end)"
	if r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code)); protocol.IsErrorReply(r) {
		t.Fatalf("LOAD: %s", r.ToBytes())
	}
	list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST", "WITHCODE"))
	arr, ok := list.(*protocol.MultiRawReply)
	if !ok || len(arr.Replies) != 1 {
		t.Fatalf("FUNCTION LIST type %T %s", list, list.ToBytes())
	}
	lib, ok := arr.Replies[0].(*protocol.MapReply)
	if !ok {
		t.Fatalf("library entry type %T", arr.Replies[0])
	}
	if list.ToBytes()[0] != '*' {
		t.Fatalf("FUNCTION LIST RESP2: %q", list.ToBytes())
	}
	wire3 := protocol.ReplyToRESP3(list)
	if wire3[0] != '*' || !bytes.Contains(wire3, []byte("%")) {
		t.Fatalf("FUNCTION LIST RESP3 want array of maps: %q", wire3)
	}
	if _, ok := lib.Data["library_code"]; !ok {
		t.Fatalf("WITHCODE missing library_code: %v", lib.Data)
	}
}

func TestResp3ACLLogEntryMaps(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	if r := server.Exec(c, utils.ToCmdLine("ACL", "LOG", "RESET")); protocol.IsErrorReply(r) {
		t.Fatalf("LOG RESET: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "resp3acl", "on", ">pw", "~*", "&*", "+@read")); protocol.IsErrorReply(r) {
		t.Fatalf("setuser: %s", r.ToBytes())
	}
	defer func() { _ = server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "resp3acl")) }()
	u := connection.NewFakeConn()
	if r := server.Exec(u, utils.ToCmdLine("AUTH", "resp3acl", "pw")); protocol.IsErrorReply(r) {
		t.Fatalf("auth: %s", r.ToBytes())
	}
	_ = server.Exec(u, utils.ToCmdLine("SET", "k", "1"))

	log := server.Exec(c, utils.ToCmdLine("ACL", "LOG"))
	arr, ok := log.(*protocol.MultiRawReply)
	if !ok || len(arr.Replies) < 1 {
		t.Fatalf("ACL LOG type %T %s", log, log.ToBytes())
	}
	entry, ok := arr.Replies[0].(*protocol.MapReply)
	if !ok {
		t.Fatalf("ACL LOG entry type %T", arr.Replies[0])
	}
	if _, ok := entry.Data["age-seconds"].(*protocol.DoubleReply); !ok {
		t.Fatalf("age-seconds should be Double, got %T", entry.Data["age-seconds"])
	}
	wire3 := protocol.ReplyToRESP3(log)
	if wire3[0] != '*' || !bytes.Contains(wire3, []byte("%")) {
		t.Fatalf("ACL LOG RESP3 want array of maps: %q", wire3)
	}
}

func TestResp3ProbabilisticInfoMaps(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "bf", "0.01", "100")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYDIM", "cms", "100", "5")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "tk", "3")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td")), "OK")

	for _, cmd := range [][]string{
		{"BF.INFO", "bf"},
		{"CMS.INFO", "cms"},
		{"TOPK.INFO", "tk"},
		{"TDIGEST.INFO", "td"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(cmd...))
		m, ok := r.(*protocol.MapReply)
		if !ok {
			t.Fatalf("%v type %T %s", cmd, r, r.ToBytes())
		}
		if r.ToBytes()[0] != '*' {
			t.Fatalf("%v RESP2: %q", cmd, r.ToBytes())
		}
		if protocol.ReplyToRESP3(r)[0] != '%' {
			t.Fatalf("%v RESP3: %q", cmd, protocol.ReplyToRESP3(r))
		}
		if len(m.Data) == 0 {
			t.Fatalf("%v empty map", cmd)
		}
	}
	tk := db.Exec(nil, utils.ToCmdLine("TOPK.INFO", "tk")).(*protocol.MapReply)
	if _, ok := tk.Data["decay"].(*protocol.DoubleReply); !ok {
		t.Fatalf("TOPK.INFO decay should be Double, got %T", tk.Data["decay"])
	}
}

func TestResp3FTConfigGetMap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "MINPREFIX"))
	if _, ok := r.(*protocol.MapReply); !ok {
		t.Fatalf("FT.CONFIG GET type %T", r)
	}
	if protocol.ReplyToRESP3(r)[0] != '%' {
		t.Fatalf("FT.CONFIG GET RESP3: %q", protocol.ReplyToRESP3(r))
	}
}
