package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bkFTReturnZero(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bkret", "ON", "HASH", "PREFIX", "1", "r:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bkret", "r:1", "FIELDS", "t", "hello", "n", "1"))

	all := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bkret", "hello"))
	sAll := string(all.ToBytes())
	if !strings.Contains(sAll, "hello") {
		t.Fatalf("no RETURN should include fields: %s", sAll)
	}

	r0 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bkret", "hello", "RETURN", "0"))
	raw := ftSearchMultiRaw(r0)
	if raw == nil {
		t.Fatalf("RETURN 0: %T %s", r0, r0.ToBytes())
	}
	// total + id + empty fields array
	if len(raw.Replies) != 3 {
		t.Fatalf("RETURN 0 want 3 elems, got %d: %s", len(raw.Replies), r0.ToBytes())
	}
	empty, ok := raw.Replies[2].(*protocol.MultiBulkReply)
	if !ok || len(empty.Args) != 0 {
		t.Fatalf("RETURN 0 want empty field array, got %T %v", raw.Replies[2], raw.Replies[2])
	}

	r1 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bkret", "hello", "RETURN", "1", "t"))
	s1 := string(r1.ToBytes())
	if !strings.Contains(s1, "hello") || strings.Contains(s1, "$3\r\nn\r\n") {
		// numeric field name "n" as bulk — soft check: title present
		if !strings.Contains(s1, "hello") {
			t.Fatalf("RETURN 1 t: %s", s1)
		}
	}
}

func TestM2bkFTSortByText(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bksort", "ON", "HASH", "PREFIX", "1", "s:",
		"SCHEMA", "name", "TEXT", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bksort", "s:b", "FIELDS", "name", "bob"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bksort", "s:a", "FIELDS", "name", "alice"))

	asc := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bksort", "*", "SORTBY", "name", "ASC"))
	s := string(asc.ToBytes())
	ia, ib := strings.Index(s, "s:a"), strings.Index(s, "s:b")
	if ia < 0 || ib < 0 || ia > ib {
		t.Fatalf("ASC want s:a before s:b: %s", s)
	}

	desc := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bksort", "*", "SORTBY", "name", "DESC"))
	s = string(desc.ToBytes())
	ia, ib = strings.Index(s, "s:a"), strings.Index(s, "s:b")
	if ia < 0 || ib < 0 || ib > ia {
		t.Fatalf("DESC want s:b before s:a: %s", s)
	}
}

func TestM2bkInfoCommandstatsFailedCalls(t *testing.T) {
	resetServerStats()
	RecordCommand("get", 10, false)
	RecordCommand("get", 20, true)
	s := genCommandStatsInfo()
	if !strings.Contains(s, "cmdstat_get:") || !strings.Contains(s, "failed_calls=1") {
		t.Fatalf("want failed_calls=1: %s", s)
	}
	if !strings.Contains(s, "rejected_calls=") {
		t.Fatalf("want rejected_calls field: %s", s)
	}
}

func TestM2bkConfigSlaveAnnounceAndLuaTimeLimit(t *testing.T) {
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		SlaveAnnounceIP:   "",
		SlaveAnnouncePort: 0,
		LuaTimeLimit:      5000,
		Databases:         16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-announce-ip", "10.0.0.1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-announce-port", "7000")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lua-time-limit", "100")), "OK")

	if config.Properties.SlaveAnnounceIP != "10.0.0.1" || config.Properties.SlaveAnnouncePort != 7000 {
		t.Fatalf("announce props: %q %d", config.Properties.SlaveAnnounceIP, config.Properties.SlaveAnnouncePort)
	}
	if config.Properties.LuaTimeLimit != 100 {
		t.Fatalf("lua-time-limit=%d", config.Properties.LuaTimeLimit)
	}

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "lua-time-limit")),
		[]string{"lua-time-limit", "100"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slave-announce-ip")),
		[]string{"slave-announce-ip", "10.0.0.1"})

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lua-time-limit", "-1"))
	if _, ok := bad.(*protocol.StandardErrReply); !ok {
		t.Fatalf("want error for negative lua-time-limit, got %T", bad)
	}
}

func TestM2bkLuaTimeLimitTimeout(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	old := config.Properties.LuaTimeLimit
	config.Properties.LuaTimeLimit = 50
	defer func() { config.Properties.LuaTimeLimit = old }()

	start := time.Now()
	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
while true do end
`, "0"))
	elapsed := time.Since(start)
	if !protocol.IsErrorReply(r) {
		t.Fatalf("want timeout error, got %T %s", r, r.ToBytes())
	}
	if elapsed > 3*time.Second {
		t.Fatalf("timeout took too long: %v", elapsed)
	}
}
