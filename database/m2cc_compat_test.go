package database

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2ccConfigReplicaAnnouncedSetProcTitleAlwaysShowLogoLuaReplicate(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:            16,
		ReplicaAnnounced:     true,
		SetProcTitle:         true,
		LuaReplicateCommands: true,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	cases := []struct {
		key, def, set string
	}{
		{"replica-announced", "yes", "no"},
		{"set-proc-title", "yes", "no"},
		{"always-show-logo", "no", "yes"},
		{"lua-replicate-commands", "yes", "no"},
	}
	for _, tc := range cases {
		asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", tc.key)),
			[]string{tc.key, tc.def})
		asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", tc.key, tc.set)), "OK")
		asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", tc.key)),
			[]string{tc.key, tc.set})
	}
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-announced", "maybe"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid replica-announced")
	}
}

func TestM2ccScriptAndFunctionKillNotBusy(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)

	r := db.Exec(nil, utils.ToCmdLine("SCRIPT", "KILL"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "NOTBUSY") {
		t.Fatalf("SCRIPT KILL want NOTBUSY: %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("FUNCTION", "KILL"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "NOTBUSY") {
		t.Fatalf("FUNCTION KILL want NOTBUSY: %s", r.ToBytes())
	}
}

func TestM2ccInfoTotalReadsWritesProcessed(t *testing.T) {
	oldReads := atomic.LoadUint64(&serverStats.TotalReadsProcessed)
	oldWrites := atomic.LoadUint64(&serverStats.TotalWritesProcessed)

	RecordCommand("get", 1, false)
	RecordCommand("set", 1, false)

	if atomic.LoadUint64(&serverStats.TotalReadsProcessed) != oldReads+1 {
		t.Fatalf("reads: got %d want %d", serverStats.TotalReadsProcessed, oldReads+1)
	}
	if atomic.LoadUint64(&serverStats.TotalWritesProcessed) != oldWrites+1 {
		t.Fatalf("writes: got %d want %d", serverStats.TotalWritesProcessed, oldWrites+1)
	}

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "total_reads_processed:") || !strings.Contains(s, "total_writes_processed:") {
		t.Fatalf("missing reads/writes fields: %s", s)
	}
}

func TestM2ccLuaSScanSetrespMembersSet(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('SSCAN', KEYS[1], '0', 'COUNT', '10')
local members = t[2]
local n = 0
for _ in ipairs(members) do n = n + 1 end
table.sort(members)
return tostring(n) .. ':' .. members[1] .. ':' .. members[2] .. ':' .. members[3]
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "3:a:b:c")
}
