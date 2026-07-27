package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2byLuaHValsSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('HVALS', KEYS[1])
return tostring(t['1'] == true) .. ':' .. tostring(t['2'] == true)
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "true:true")
}

func TestM2byLuaSRandMemberSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('SRANDMEMBER', KEYS[1], 2)
local n = 0
for _ in pairs(t) do n = n + 1 end
return tostring(n)
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "2")
}

func TestM2byInfoTotalSystemMemory(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T %s", r, r.ToBytes())
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "total_system_memory:") || !strings.Contains(s, "total_system_memory_human:") {
		t.Fatalf("missing total_system_memory fields: %s", s)
	}
	// On Windows/Linux CI we expect non-zero; other platforms may report 0.
	if getTotalSystemMemoryBytes() > 0 && !strings.Contains(s, "total_system_memory:0") {
		// ok — field present with OS memory
	} else if getTotalSystemMemoryBytes() == 0 {
		if !strings.Contains(s, "total_system_memory:0") {
			t.Fatalf("want total_system_memory:0 on unsupported platform: %s", s)
		}
	}
}

func TestM2byClientListCapabilities(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetProtocolVersion(3)
	line := formatClientListLine(c)
	if !strings.Contains(line, "capabilities=resp3") {
		t.Fatalf("want capabilities=resp3: %q", line)
	}

	c2 := connection.NewFakeConn()
	c2.SetProtocolVersion(2)
	line2 := formatClientListLine(c2)
	if !strings.Contains(line2, "capabilities=") {
		t.Fatalf("want capabilities= field: %q", line2)
	}
	if strings.Contains(line2, "capabilities=resp3") {
		t.Fatalf("resp2 should not have resp3 cap: %q", line2)
	}
}

func TestM2byACLListDefaultChannels(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	list := server.Exec(c, utils.ToCmdLine("ACL", "LIST"))
	multi, ok := list.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL LIST: %T %s", list, list.ToBytes())
	}
	found := false
	for _, line := range multi.Args {
		s := string(line)
		if strings.Contains(s, "user default") && strings.Contains(s, "&*") && strings.Contains(s, "~*") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL LIST missing default &*/~*: %s", list.ToBytes())
	}
}

func TestM2byACLDryRunPSubscribe(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "psu", "on", "nopass", "+psubscribe", "resetchannels", "&news:*",
	)), "OK")

	ok := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "psu", "PSUBSCRIBE", "news:*"))
	asserts.AssertStatusReply(t, ok, "OK")

	deny := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "psu", "PSUBSCRIBE", "other:*"))
	if !protocol.IsErrorReply(deny) || !strings.Contains(string(deny.ToBytes()), "channel") {
		t.Fatalf("want channel deny: %s", deny.ToBytes())
	}
}
