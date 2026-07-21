package database

import (
	"strconv"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2ExpireFlags(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "100", "NX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "200", "NX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "50", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "1000", "GT")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "LT")), 1)
}

func TestM2SetOptions(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "1", "EX", "100")), "OK")
	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "s")).(*protocol.IntReply).Code
	if ttl <= 0 || ttl > 100 {
		t.Fatalf("ttl=%d", ttl)
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "2", "KEEPTTL")), "OK")
	ttl2 := db.Exec(nil, utils.ToCmdLine("TTL", "s")).(*protocol.IntReply).Code
	if ttl2 <= 0 {
		t.Fatalf("KEEPTTL lost ttl: %d", ttl2)
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "3", "GET")), "2")

	exat := time.Now().Add(40 * time.Second).Unix()
	cmd := append(utils.ToCmdLine("SET", "s2", "x", "EXAT"), []byte(strconv.FormatInt(exat, 10)))
	asserts.AssertStatusReply(t, db.Exec(nil, cmd), "OK")
	if db.Exec(nil, utils.ToCmdLine("TTL", "s2")).(*protocol.IntReply).Code <= 0 {
		t.Fatal("EXAT failed")
	}
}

func TestM2SMoveLPosZRangeStore(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "a", "m1", "m2"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "a", "b", "m1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b", "m1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "a", "m1")), 0)

	db.Exec(nil, utils.ToCmdLine("RPUSH", "l", "x", "y", "x", "z"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "l", "x")), 0)
	cnt := db.Exec(nil, utils.ToCmdLine("LPOS", "l", "x", "COUNT", "0"))
	mb, ok := cnt.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 2 {
		t.Fatalf("LPOS COUNT: %T %s", cnt, cnt.ToBytes())
	}

	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "z2", "z", "0", "1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "z2")), 2)
	rm := db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "z"))
	if _, ok := rm.(*protocol.BulkReply); !ok {
		t.Fatalf("ZRANDMEMBER: %T", rm)
	}
}

func TestM2UnWatch(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	db.Exec(c, utils.ToCmdLine("WATCH", "k"))
	if len(c.GetWatching()) != 1 {
		t.Fatal("watch not set")
	}
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("UNWATCH")), "OK")
	if len(c.GetWatching()) != 0 {
		t.Fatal("unwatch failed")
	}
}

func TestM2SPopBulk(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "s", "a"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "s")), "a")
}

func TestM2HExpireTime(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "60", "f"))
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRETIME", "h", "FIELDS", "1", "f"))
	mr, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("HEXPIRETIME: %T %s", reply, reply.ToBytes())
	}
	ir, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || ir.Code <= 0 {
		t.Fatalf("expire time: %#v", mr.Replies[0])
	}
}
