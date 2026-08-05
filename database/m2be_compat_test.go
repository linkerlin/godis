package database

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2beConfigLogFile(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := config.Properties.LogFile
	defer func() {
		_ = logger.ReconfigureOutput(old)
		config.Properties.LogFile = old
	}()

	dir := t.TempDir()
	path := filepath.Join(dir, "godis-m2be.log")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "logfile", path)), "OK")
	if config.Properties.LogFile != path {
		t.Fatalf("LogFile want %q got %q", path, config.Properties.LogFile)
	}
	logger.Info("m2be logfile probe")
	deadline := time.Now().Add(2 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), "m2be logfile probe") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("logfile not written: err=%v content=%q", err, string(data))
		}
		time.Sleep(10 * time.Millisecond)
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "logfile", "")), "OK")
}

func TestM2beFTReturnASAndWithCursor(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2be", "ON", "HASH", "PREFIX", "1", "b:", "SCHEMA", "title", "TEXT", "body", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "b:1", "title", "hello", "body", "world"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2be", "b:1", "FIELDS", "title", "hello", "body", "world"))

	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2be", "hello", "RETURN", "1", "title", "AS", "t"))
	s := string(r.ToBytes())
	if strings.Contains(s, "$2\r\nAS\r\n") || strings.Contains(s, "$2\r\nas\r\n") {
		t.Fatalf("AS must not be a field name: %s", s)
	}
	if !strings.Contains(s, "$1\r\nt\r\n") {
		t.Fatalf("expected alias t in reply: %s", s)
	}

	// FT.SEARCH WITHCURSOR is implemented (reuses FT.CURSOR table); see
	// TestFTSearchWithCursorPages.
	searchCur := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2be", "hello", "WITHCURSOR", "COUNT", "1"))
	if protocol.IsErrorReply(searchCur) {
		t.Fatalf("SEARCH WITHCURSOR should be supported: %s", searchCur.ToBytes())
	}
	// FT.AGGREGATE WITHCURSOR is implemented in RediSearch Phase B: see
	// TestFTAggregateWithCursorPages in redisearch_phase_b_test.go.
	agg := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "m2be", "*", "WITHCURSOR"))
	if protocol.IsErrorReply(agg) {
		t.Fatalf("AGGREGATE WITHCURSOR should be supported: %s", agg.ToBytes())
	}
}

func TestM2beInfoMaxmemory(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldMem := config.Properties.Maxmemory
	oldPol := config.Properties.MaxmemoryPolicy
	defer func() {
		config.Properties.Maxmemory = oldMem
		config.Properties.MaxmemoryPolicy = oldPol
	}()
	config.Properties.Maxmemory = 1048576
	config.Properties.MaxmemoryPolicy = "allkeys-lru"

	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T %s", r, r.ToBytes())
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "maxmemory:1048576") ||
		!strings.Contains(s, "maxmemory_human:") ||
		!strings.Contains(s, "maxmemory_policy:allkeys-lru") {
		t.Fatalf("INFO memory missing maxmemory fields: %s", s)
	}
}

func TestM2beClientListIDAndResp(t *testing.T) {
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	line := formatClientListLine(c)
	if !strings.Contains(line, "resp=") {
		t.Fatalf("missing resp=: %q", line)
	}

	idStr := strconv.FormatInt(c.GetClientID(), 10)
	r := execClientListConn(c, [][]byte{[]byte("ID"), []byte(idStr)})
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT LIST ID: %T %s", r, r.ToBytes())
	}
	out := string(bulk.Arg)
	if !strings.Contains(out, "id="+idStr) {
		t.Fatalf("ID filter miss: %q", out)
	}
	r = execClientListConn(c, [][]byte{[]byte("ID"), []byte("999999999")})
	bulk, ok = r.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) != 0 {
		t.Fatalf("ID filter should empty for unknown: %s", r.ToBytes())
	}
}
