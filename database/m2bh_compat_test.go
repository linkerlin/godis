package database

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bhClientListWatchingAndFlags(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("WATCH", "k1")), "OK")
	line := formatClientListLine(c)
	if !strings.Contains(line, "watch=1") {
		t.Fatalf("want watch=1, got %q", line)
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("UNWATCH")), "OK")
	line = formatClientListLine(c)
	if !strings.Contains(line, "watch=0") {
		t.Fatalf("want watch=0, got %q", line)
	}

	id := EnableTracking(c, "ON", nil, "", false)
	c.SetTrackingID(id)
	line = formatClientListLine(c)
	if !strings.Contains(line, "flags=") || !strings.Contains(line, "t") {
		t.Fatalf("want tracking flag t, got %q", line)
	}
	// flags field itself should contain t (e.g. Nt or t)
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, "flags=") {
			if !strings.Contains(part, "t") {
				t.Fatalf("flags missing t: %q", part)
			}
			break
		}
	}
}

func TestM2bhConfigAclfileAndReplicaReadOnly(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldACL := config.Properties.AclFile
	oldRO := config.Properties.ReplicaReadOnly
	defer func() {
		config.Properties.AclFile = oldACL
		config.Properties.ReplicaReadOnly = oldRO
	}()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aclfile", "users.acl")), "OK")
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "aclfile"))
	if val, ok := configReplyValue(r, "aclfile"); !ok || val != "users.acl" {
		t.Fatalf("CONFIG GET aclfile: %s", r.ToBytes())
	}

	config.Properties.ReplicaReadOnly = true
	atomic.StoreInt32(&server.role, slaveRole)
	defer atomic.StoreInt32(&server.role, 0)
	deny := server.Exec(c, utils.ToCmdLine("SET", "ro", "1"))
	if !protocol.IsErrorReply(deny) || !strings.Contains(string(deny.ToBytes()), "READONLY") {
		t.Fatalf("replica-read-only yes: %s", deny.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-read-only", "no")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ro", "1")), "OK")
	r = server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slave-read-only"))
	if val, ok := configReplyValue(r, "slave-read-only"); !ok || val != "no" {
		t.Fatalf("slave-read-only alias: %s", r.ToBytes())
	}
}

func TestM2bhInfoTotalErrorReplies(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	atomic.StoreUint64(&serverStats.TotalErrorReplies, 0)
	defer atomic.StoreUint64(&serverStats.TotalErrorReplies, 0)

	_ = server.Exec(c, utils.ToCmdLine("GET")) // arity error
	if atomic.LoadUint64(&serverStats.TotalErrorReplies) == 0 {
		t.Fatal("expected total_error_replies > 0")
	}
	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "total_error_replies:") {
		t.Fatalf("INFO stats: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "RESETSTAT")), "OK")
	if atomic.LoadUint64(&serverStats.TotalErrorReplies) != 0 {
		t.Fatal("RESETSTAT should clear total_error_replies")
	}
}

func TestM2bhFTTimeout(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bh", "ON", "HASH", "PREFIX", "1", "h:", "SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bh", "h:1", "FIELDS", "t", "hello"))

	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bh", "hello", "TIMEOUT", "50"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TIMEOUT 50 should succeed: %s", r.ToBytes())
	}

	engine := searchEngines["m2bh"]
	if engine == nil {
		t.Fatal("missing engine")
	}
	_, err := engine.Search("hello", &redisearch.SearchOptions{
		Deadline: time.Now().Add(-time.Millisecond),
	})
	if err != redisearch.ErrTimeout {
		t.Fatalf("past deadline want ErrTimeout, got %v", err)
	}
	_, err = engine.Aggregate(&redisearch.AggregationRequest{
		Query:    "*",
		Deadline: time.Now().Add(-time.Millisecond),
	})
	if err != redisearch.ErrTimeout {
		t.Fatalf("aggregate past deadline: %v", err)
	}
}
