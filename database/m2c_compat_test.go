package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2cZRankWithScore(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "a", "2", "b"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "z", "a")), 0)
	r := db.Exec(nil, utils.ToCmdLine("ZRANK", "z", "a", "WITHSCORE"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("WITHSCORE: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 0)
	asserts.AssertBulkReply(t, mr.Replies[1], "1.5")
}

func TestM2cScanFiltersExpired(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "alive", "1"))
	db.Exec(nil, utils.ToCmdLine("SET", "dead", "1", "PX", "1"))
	time.Sleep(5 * time.Millisecond)
	r := db.Exec(nil, utils.ToCmdLine("SCAN", "0", "MATCH", "*"))
	mr := r.(*protocol.MultiRawReply)
	keys := mr.Replies[1].(*protocol.MultiBulkReply)
	for _, k := range keys.Args {
		if string(k) == "dead" {
			t.Fatal("SCAN returned expired key")
		}
	}
}

func TestM2cHTTLFields(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f1", "v", "f2", "v"))
	db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "60", "FIELDS", "1", "f1"))
	r := db.Exec(nil, utils.ToCmdLine("HTTL", "h", "FIELDS", "2", "f1", "f2"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("HTTL FIELDS: %T", r)
	}
	t1 := mr.Replies[0].(*protocol.IntReply).Code
	t2 := mr.Replies[1].(*protocol.IntReply).Code
	if t1 <= 0 || t2 != -1 {
		t.Fatalf("ttl f1=%d f2=%d", t1, t2)
	}
}

func TestM2cGetEXAT(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	exat := time.Now().Add(30 * time.Second).Unix()
	cmd := append(utils.ToCmdLine("GETEX", "k", "EXAT"), []byte(strconv.FormatInt(exat, 10)))
	asserts.AssertBulkReply(t, db.Exec(nil, cmd), "v")
	if db.Exec(nil, utils.ToCmdLine("TTL", "k")).(*protocol.IntReply).Code <= 0 {
		t.Fatal("GETEX EXAT did not set TTL")
	}
}

func TestM2cHRandFieldSingle(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "missing")))
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	r := db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "h"))
	asserts.AssertBulkReply(t, r, "f")
}

func TestM2cLPopCountMissing(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("LPOP", "nokey", "2"))
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		t.Fatalf("want empty array, got %T %s", r, r.ToBytes())
	}
}
