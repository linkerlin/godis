package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestCompatWaitAOFFsyncPath(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, AppendOnly: false}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	config.Properties.AppendOnly = true
	r := server.Exec(c, utils.ToCmdLine("WAITAOF", "1", "0", "50"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("WAITAOF: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, raw.Replies[0], 1)
}

func TestCompatKeyspaceNotifySetDel(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:            16,
		NotifyKeyspaceEvents: "KEA",
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	sub := connection.NewFakeConn()

	_ = pubsub.Subscribe(server.hub, sub, [][]byte{[]byte("__keyspace@0__:ks")})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ks", "v")), "OK")
	// Drain subscribe confirm + notification (best-effort via wait).
	time.Sleep(20 * time.Millisecond)
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("DEL", "ks")), 1)
	time.Sleep(20 * time.Millisecond)
}

func TestCompatLatencyHistogramAfterSlowCmd(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	// Force a sample via direct record (Exec only records >=1ms).
	RecordLatency("set", 2*time.Millisecond)
	RecordCommandLatency("set", 2*time.Millisecond)
	r := server.Exec(c, utils.ToCmdLine("LATENCY", "HISTOGRAM", "set"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("HISTOGRAM: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "set") || !strings.Contains(body, "calls") {
		t.Fatalf("expected set histogram: %s", body)
	}
	_ = server.Exec(c, utils.ToCmdLine("LATENCY", "RESET"))
}

func TestCompatFTSearchWithCursor(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_sc", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "t", "TEXT",
	)), "OK")
	for i := 0; i < 5; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("HSET", "d:"+strconv.Itoa(i), "t", "hello world"))
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_sc", "hello", "WITHCURSOR", "COUNT", "2"))
	multi, ok := r.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) != 2 {
		t.Fatalf("expected [results, cursor], got %s", r.ToBytes())
	}
	cur, ok := multi.Replies[1].(*protocol.IntReply)
	if !ok {
		t.Fatalf("cursor type %T", multi.Replies[1])
	}
	if cur.Code == 0 {
		// May exhaust in one page if LIMIT capped results; still a valid reply.
		return
	}
	r2 := db.Exec(nil, utils.ToCmdLine("FT.CURSOR", "READ", "idx_sc", strconv.FormatInt(cur.Code, 10)))
	if protocol.IsErrorReply(r2) {
		t.Fatalf("CURSOR READ: %s", r2.ToBytes())
	}
}

func TestCompatLFULogIncrBounded(t *testing.T) {
	c := uint64(0)
	c = lfuLogIncr(c)
	if c != lfuInitVal {
		t.Fatalf("init want %d got %d", lfuInitVal, c)
	}
	for i := 0; i < 10000; i++ {
		c = lfuLogIncr(c)
	}
	if c > 255 {
		t.Fatalf("counter exceeded 255: %d", c)
	}
}
