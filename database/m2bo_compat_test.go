package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2boInfoKeyspaceAvgTTLAndSubexpiry(t *testing.T) {
	// Isolate from suite-global config (AOF/RDB left on by other tests would
	// load foreign keys and skew avg_ttl / key counts).
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:  16,
		AppendOnly: false,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	// Ensure empty DB even if RDB/AOF load path was somehow armed.
	_ = server.Exec(c, utils.ToCmdLine("FLUSHALL"))

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "k", "v", "EX", "10")), "OK")
	_ = server.Exec(c, utils.ToCmdLine("HSET", "h", "f", "1"))
	_ = server.Exec(c, utils.ToCmdLine("HEXPIRE", "h", "30", "FIELDS", "1", "f"))

	r := server.Exec(c, utils.ToCmdLine("INFO", "keyspace"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO keyspace: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "subexpiry=") {
		t.Fatalf("want subexpiry: %s", s)
	}
	// Parse avg_ttl — should be milliseconds (~10000 for EX 10), not microseconds (~10_000_000)
	for _, line := range strings.Split(s, "\r\n") {
		if !strings.HasPrefix(line, "db0:") {
			continue
		}
		for _, part := range strings.Split(line, ",") {
			if strings.HasPrefix(part, "avg_ttl=") {
				v, _ := strconv.ParseInt(strings.TrimPrefix(part, "avg_ttl="), 10, 64)
				if v < 1000 || v > 15_000 {
					t.Fatalf("avg_ttl should be ~ms for EX 10, got %d in %s", v, line)
				}
			}
			if strings.HasPrefix(part, "subexpiry=") {
				v, _ := strconv.Atoi(strings.TrimPrefix(part, "subexpiry="))
				if v < 1 {
					t.Fatalf("want subexpiry>=1, got %d in %s", v, line)
				}
			}
		}
	}
}

func TestM2boClientListReplyOffFlag(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetReplyMode("OFF")
	line := formatClientListLine(c)
	if !strings.Contains(line, "flags=") || !strings.Contains(line, "n") {
		t.Fatalf("want flag n: %q", line)
	}
	// Ensure lowercase n is in flags field
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, "flags=") {
			if !strings.Contains(part, "n") {
				t.Fatalf("flags missing n: %q", part)
			}
			if part == "flags=N" {
				t.Fatalf("reply-off should not be bare N: %q", part)
			}
		}
	}
	c.SetReplyMode("ON")
	line = formatClientListLine(c)
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, "flags=") && strings.Contains(part, "n") && part != "flags=N" {
			// bare N is ok; lowercase n alone or with others after ON should not appear
			if strings.Contains(strings.TrimPrefix(part, "flags="), "n") &&
				strings.TrimPrefix(part, "flags=") != "N" {
				t.Fatalf("after REPLY ON, unexpected n: %q", part)
			}
		}
	}
}

func TestM2boFTOnTimeout(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "RETURN")), "OK")
	r := ftTimeoutReply(redisearch.ErrTimeout)
	if protocol.IsErrorReply(r) {
		t.Fatalf("ON_TIMEOUT RETURN should not error: %s", r.ToBytes())
	}
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 1 {
		t.Fatalf("RETURN empty search shape: %T %s", r, r.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "FAIL")), "OK")
	r = ftTimeoutReply(redisearch.ErrTimeout)
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Timeout") {
		t.Fatalf("ON_TIMEOUT FAIL want Timeout err: %s", r.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "BOGUS"))
	if !protocol.IsErrorReply(bad) {
		t.Fatal("want reject BOGUS")
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "ON_TIMEOUT")),
		[]string{"ON_TIMEOUT", "FAIL"})
}

func TestM2boLuaZRangeWithScoresMap(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZRANGE', KEYS[1], '0', '-1', 'WITHSCORES')
return type(t) .. ':' .. tostring(t['a'])
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "table:1")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(2)
local t = redis.call('ZRANGE', KEYS[1], '0', '-1', 'WITHSCORES')
return type(t) .. ':' .. tostring(#t)
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "table:4")
}
