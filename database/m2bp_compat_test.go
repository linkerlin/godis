package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bpObjectZSetListpack(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "z")), "listpack")

	for i := 0; i < 130; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("ZADD", "zbig", "1", "m"+utils.RandString(8)))
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "zbig")), "skiplist")
}

func TestM2bpFunctionHelp(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("FUNCTION HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "LOAD") || !strings.Contains(joined, "HELP") {
		t.Fatalf("help missing LOAD/HELP: %s", joined)
	}
}

func bytesJoin(args [][]byte) string {
	var b strings.Builder
	for _, a := range args {
		b.Write(a)
		b.WriteByte(' ')
	}
	return b.String()
}

func TestM2bpMemoryStatsAndInfoDataset(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "k", "v"))
	_ = server.Exec(c, utils.ToCmdLine("EXPIRE", "k", "60"))

	r := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", r)
	}
	keys := map[string]bool{}
	for i := 0; i+1 < len(raw.Replies); i += 2 {
		if b, ok := raw.Replies[i].(*protocol.BulkReply); ok {
			keys[string(b.Arg)] = true
		}
	}
	for _, want := range []string{"fragmentation.bytes", "overhead.hashtable.expires"} {
		if !keys[want] {
			t.Fatalf("missing %q", want)
		}
	}

	info := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := info.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", info)
	}
	s := string(bulk.Arg)
	for _, want := range []string{"used_memory_dataset:", "used_memory_overhead:", "used_memory_dataset_perc:"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}
}

func TestM2bpLuaZPopAndHRandFieldMap(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "f1", "v1", "f2", "v2"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZPOPMIN', KEYS[1])
return type(t) .. ':' .. tostring(t['a'] ~= nil or t['b'] ~= nil)
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "table:true")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('HRANDFIELD', KEYS[1], '2', 'WITHVALUES')
return type(t)
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "table")
}
