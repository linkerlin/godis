package database

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bfInfoEvictedKeys(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	atomic.StoreUint64(&serverStats.EvictedKeys, 7)
	defer atomic.StoreUint64(&serverStats.EvictedKeys, 0)

	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	if strings.Contains(s, "evict_keys:") {
		t.Fatalf("legacy evict_keys still present: %s", s)
	}
	if !strings.Contains(s, "evicted_keys:7") {
		t.Fatalf("want evicted_keys:7, got %s", s)
	}
}

func TestM2bfAppendFsyncRuntime(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "appendonly.aof")
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofPath,
		AppendFsync:    aof.FsyncEverySec,
		Databases:      16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	if server.persister == nil {
		t.Fatal("expected AOF persister")
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendfsync", "always")), "OK")
	st := server.persister.Stats()
	if st["fsync"] != aof.FsyncAlways {
		t.Fatalf("persister fsync want always, got %v", st["fsync"])
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendfsync", "no")), "OK")
	st = server.persister.Stats()
	if st["fsync"] != aof.FsyncNo {
		t.Fatalf("persister fsync want no, got %v", st["fsync"])
	}
}

func TestM2bfPidFile(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := config.Properties.PidFile
	defer func() {
		config.Properties.PidFile = old
	}()

	path := filepath.Join(t.TempDir(), "godis.pid")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "pidfile", path)), "OK")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(string(data))
	want := strconv.Itoa(os.Getpid())
	if got != want {
		t.Fatalf("pidfile content %q want %q", got, want)
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "pidfile", "")), "OK")
}

func TestM2bfClientListCmd(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	_ = server.Exec(c, utils.ToCmdLine("SET", "m2bf", "1"))
	line := formatClientListLine(c)
	if !strings.Contains(line, "cmd=set") {
		t.Fatalf("want cmd=set, got %q", line)
	}
	_ = server.Exec(c, utils.ToCmdLine("GET", "m2bf"))
	line = formatClientListLine(c)
	if !strings.Contains(line, "cmd=get") {
		t.Fatalf("want cmd=get, got %q", line)
	}
}

// TestM2bfFTAggregateApply verifies FT.AGGREGATE APPLY is now supported
// (RediSearch Phase B); see TestFTAggregateApplyExpression in
// redisearch_phase_b_test.go for expression-value coverage.
func TestM2bfFTAggregateApply(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bf", "ON", "HASH", "PREFIX", "1", "f:", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bf", "f:1", "FIELDS", "n", "10"))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "m2bf", "*", "APPLY", "@n/2", "AS", "half",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("APPLY should be supported: %s", r.ToBytes())
	}
	if !strings.Contains(string(r.ToBytes()), "half") {
		t.Fatalf("expected computed field 'half' in reply: %s", r.ToBytes())
	}
}
