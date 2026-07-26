package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2blClientListMemFields(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetClientName("bob")
	c.SetLastCommand("get")
	line := formatClientListLine(c)
	if !strings.Contains(line, "argv-mem=") || !strings.Contains(line, "multi-mem=0") || !strings.Contains(line, "tot-mem=") {
		t.Fatalf("want mem fields: %q", line)
	}
	if !strings.Contains(line, "argv-mem=3") {
		t.Fatalf("argv-mem for get: %q", line)
	}

	c.SetMultiState(true)
	c.EnqueueCmd([][]byte{[]byte("SET"), []byte("k"), []byte("vv")})
	line = formatClientListLine(c)
	if !strings.Contains(line, "multi-mem=") {
		t.Fatalf("missing multi-mem: %q", line)
	}
	// SET + k + vv = 3+1+2 = 6
	if !strings.Contains(line, "multi-mem=6") {
		t.Fatalf("want multi-mem=6: %q", line)
	}
	argv, multi, tot := clientListMemEstimates(c, "get")
	if tot < argv+multi {
		t.Fatalf("tot-mem %d < argv+multi %d+%d", tot, argv, multi)
	}
}

func TestM2blFTDialectAndMaxSearchResults(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2blft", "ON", "HASH", "PREFIX", "1", "f:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	for i := 0; i < 5; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2blft", "f:"+string(rune('a'+i)), "FIELDS", "t", "hello"))
	}

	bad := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2blft", "hello", "DIALECT", "99"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "DIALECT") {
		t.Fatalf("want Invalid DIALECT: %s", bad.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXSEARCHRESULTS", "2")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2blft", "hello", "LIMIT", "0", "10"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("SEARCH: %T %s", r, r.ToBytes())
	}
	// total + 2*(id+fields) = 5
	if len(raw.Replies) != 5 {
		t.Fatalf("MAXSEARCHRESULTS 2: want 5 elems, got %d", len(raw.Replies))
	}
	// restore
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXSEARCHRESULTS", "10000"))
}

func TestM2blLuaSetrespOutput(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(3); return false`, "0"))
	if _, ok := r.(*protocol.BooleanReply); !ok {
		t.Fatalf("setresp(3) false want BooleanReply, got %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(3); return nil`, "0"))
	if _, ok := r.(*protocol.NullReply); !ok {
		t.Fatalf("setresp(3) nil want NullReply, got %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(3); return {a=1}`, "0"))
	if _, ok := r.(*protocol.MapReply); !ok {
		t.Fatalf("setresp(3) map want MapReply, got %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(2); return false`, "0"))
	asserts.AssertIntReply(t, r, 0)
}

func TestM2blConfigAnnounceHost(t *testing.T) {
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		AnnounceHost: "",
		Bind:         "127.0.0.1",
		Port:         6399,
		Databases:    16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "announce-host", "example.com")), "OK")
	if config.Properties.AnnounceHost != "example.com" {
		t.Fatalf("AnnounceHost=%q", config.Properties.AnnounceHost)
	}
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "announce-host")),
		[]string{"announce-host", "example.com"})
	if config.Properties.AnnounceAddress() != "example.com:6399" {
		t.Fatalf("AnnounceAddress=%s", config.Properties.AnnounceAddress())
	}
}

func TestM2blInfoPersistenceFields(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	s := genPersistenceInfo(server)
	for _, want := range []string{
		"aof_pending_rewrite:",
		"aof_buffer_length:",
		"aof_rewrite_buffer_length:",
		"aof_pending_bio_fsync:",
		"aof_delayed_fsync:",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}
}
