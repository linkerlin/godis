package database

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2rSubscribeWhitelist(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	_ = pubsub.Subscribe(server.hub, c, [][]byte{[]byte("ch")})
	if c.SubsCount() < 1 {
		t.Fatal("expected subscribed")
	}
	bad := server.Exec(c, utils.ToCmdLine("SET", "k", "v"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "only (P)SUBSCRIBE") {
		t.Fatalf("expected subscribe-context error, got %s", bad.ToBytes())
	}
	ok := server.Exec(c, utils.ToCmdLine("PING"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("PING should be allowed: %s", ok.ToBytes())
	}
}

func TestM2rMonitorBroadcast(t *testing.T) {
	server := getTestServer()
	mon := connection.NewFakeConn()
	AddMonitorClient(mon)
	defer RemoveMonitorClient(mon)

	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "mk", "1"))
	time.Sleep(20 * time.Millisecond)
	out := string(mon.Bytes())
	if !strings.Contains(out, "set") || !strings.Contains(out, "mk") {
		t.Fatalf("monitor did not see SET: %q", out)
	}
}

func TestM2rClientUnblock(t *testing.T) {
	testDB.Flush()
	c := connection.NewFakeConn()
	id := c.GetClientID()

	var wg sync.WaitGroup
	wg.Add(1)
	var result redis.Reply
	go func() {
		defer wg.Done()
		result = testDB.Exec(c, utils.ToCmdLine("BLPOP", "m2r:blk", "2"))
	}()
	time.Sleep(50 * time.Millisecond)
	asserts.AssertIntReply(t, execClientUnblock([][]byte{
		[]byte(strconv.FormatInt(id, 10)), []byte("ERROR"),
	}), 1)
	wg.Wait()
	errReply, ok := result.(*protocol.StandardErrReply)
	if !ok || !strings.Contains(errReply.Error(), "UNBLOCKED") {
		t.Fatalf("expected UNBLOCKED, got %T %v", result, result)
	}
}

func TestM2rFTProfile(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2ridx", "ON", "HASH", "PREFIX", "1", "m2r:", "SCHEMA", "t", "TEXT",
	)), "OK")
	db.Exec(nil, utils.ToCmdLine("HSET", "m2r:1", "t", "hello"))
	r := db.Exec(nil, utils.ToCmdLine("FT.PROFILE", "m2ridx", "SEARCH", "hello"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("FT.PROFILE: %T %s", r, r.ToBytes())
	}
}
