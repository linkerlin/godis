package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 96 R4-1 extras: LINSERT/LSET/LTRIM, ZLEX*, ZUNIONSTORE WEIGHTS, ZINTERSTORE MAX,
// BITOP AND/OR, LCS, GETSET, SETEX/PSETEX, HMSET, HPEXPIRE, single SPOP/HRANDFIELD/ZRANDMEMBER,
// BLMOVE, XTRIM, SORT, ZREMRANGEBYRANK, DECRBY, PFCOUNT multi, RENAMENX, PERSIST.
func TestR41Batch96Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b96l", "a", "b", "c", "a", "b")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b96l", "AFTER", "b", "X")), 6)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b96l", "2", "Y")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b96l", "0", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b96l", "0", "-1")), []string{"a", "b", "Y", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b96zlex", "0", "aa", "0", "ab", "0", "ba", "0", "bb")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b96zlex", "[a", "[b")), []string{"aa", "ab"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b96zlex", "[a", "(b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b96zlex", "[aa", "[ab")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b96z1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b96z2", "2", "b", "4", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b96zu", "2", "b96z1", "b96z2", "WEIGHTS", "2", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b96zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "c", "6", "b", "10", "d", "12"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b96zi", "2", "b96z1", "b96z2", "AGGREGATE", "MAX")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b96zi", "0", "-1", "WITHSCORES")), []string{"b", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b96b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b96b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b96b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b96ba", "b96b1", "b96b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b96ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b96bor", "b96b1", "b96b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b96bor")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b96s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b96s2", "abxyz")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b96s1", "b96s2", "LEN")), 2)

	gs := db.Exec(nil, utils.ToCmdLine("GETSET", "b96gs", "old"))
	if _, ok := gs.(*protocol.NullBulkReply); !ok {
		t.Fatalf("GETSET miss: %T %s", gs, gs.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b96gs", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b96gs", "new")), "old")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b96sx", "60", "v")), "OK")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b96sx")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b96sx", "b96sx2")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b96ps", "5000", "w")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b96ps")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b96ps")), -1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b96h", "f1", "v1", "f4", "v4")), "OK")
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b96h", "f1", "f4", "missing"))
	asserts.AssertMultiBulkReply(t, hm, []string{"v1", "v4", ""})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b96h1", "only", "one")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b96h1")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b96set1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b96set1")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b96src", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b96dst", "x")), 1)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b96src", "b96dst", "LEFT", "RIGHT", "0"))
	asserts.AssertBulkReply(t, bl, "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b96dst", "0", "-1")), []string{"x", "a"})

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b96x", "1-0", "a", "1")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b96x", "2-0", "a", "2")), "2-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b96x", "MAXLEN", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b96sort", "3", "1", "2")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b96sort", "ALPHA")), []string{"1", "2", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b96zc", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b96zc", "1", "2")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b96zc", "0", "-1")), []string{"a", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b96dec", "5")), -5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b96dec", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b96dec", "3")), 7)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b96pfm")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b96pf", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b96pf", "b96pfm")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b96hex", "f1", "v1")), 1)
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b96hex", "4000", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hp) {
		t.Fatalf("HPEXPIRE: %s", hp.ToBytes())
	}
}
