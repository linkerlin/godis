package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 89 R4-1 extras: LTRIM, ZREMRANGEBYRANK, HSETNX, SET NX, LMOVE, ZMPOP/LMPOP,
// GEOADD/GEOHASH, *INTERCARD, EXPIRE+PERSIST, GETDEL, BITFIELD INCRBY, LPOS RANK, HMSET.
func TestR41Batch89Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b89l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b89l", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b89l", "0", "-1")), []string{"b", "c", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b89z", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b89z", "0", "1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b89z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b89h", "f", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b89h", "f", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b89h", "g", "y")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b89nx", "v", "NX")), "OK")
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b89nx", "w", "NX"))
	if _, ok := nx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX fail: %T %s", nx, nx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b89nx")), "v")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b89la", "1", "2", "3")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b89la", "b89lb", "RIGHT", "LEFT")), "3")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b89la")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b89lb")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b89zp", "1", "a", "2", "b")), 2)
	zmpop := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b89zp", "MIN"))
	if protocol.IsErrorReply(zmpop) || !strings.Contains(string(zmpop.ToBytes()), "a") {
		t.Fatalf("ZMPOP: %s", zmpop.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b89lp", "x", "y")), 2)
	lmpop := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b89lp", "LEFT"))
	if protocol.IsErrorReply(lmpop) || !strings.Contains(string(lmpop.ToBytes()), "x") {
		t.Fatalf("LMPOP: %s", lmpop.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b89g", "13.361389", "38.115556", "Palermo")), 1)
	gh := db.Exec(nil, utils.ToCmdLine("GEOHASH", "b89g", "Palermo"))
	if protocol.IsErrorReply(gh) || !strings.Contains(string(gh.ToBytes()), "sqc8") {
		t.Fatalf("GEOHASH soft: %s", gh.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b89sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b89sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b89sa", "b89sb")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b89za", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b89zb", "1", "b", "2", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b89za", "b89zb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b89za", "1", "2")), []string{"a", "b"})

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b89e", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b89e", "100")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b89e")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b89e")), -1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b89gd", "bye")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b89gd")), "bye")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b89gd")), 0)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b89bf", "INCRBY", "u8", "0", "5"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "5") {
		t.Fatalf("BITFIELD INCRBY: %s", bf.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b89lp2", "a", "b", "a", "c", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b89lp2", "a", "RANK", "2")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b89hm", "a", "1", "b", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HMGET", "b89hm", "a", "b")), []string{"1", "2"})

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b89x", "1-0", "f", "1")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b89x", "2-0", "f", "2")), "2-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b89x", "MAXLEN", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b89x")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b89sub", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b89sub", "1", "3")), "ell")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b89sub", "1", "3")), "ell")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b89pf1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b89pf2", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b89pfm", "b89pf1", "b89pf2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b89pfm")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b89miss")), "none")
}
