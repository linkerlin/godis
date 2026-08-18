package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 101 R4-1 extras: LINSERT AFTER/LSET/LTRIM, BLMOVE RIGHT LEFT, LMOVE,
// LPOS MAXLEN, ZLEX*, ZUNIONSTORE WEIGHTS+MIN, ZINTERSTORE MAX, ZDIFFSTORE,
// BITOP AND/OR/DIFF, LCS string, GETSET, SETEX/PSETEX/KEEPTTL, HPEXPIRE,
// HGETDEL, ZPOPMIN, ZREMRANGEBYSCORE, SORT ALPHA DESC, XTRIM MINID.
func TestR41Batch101Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b101l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b101l", "AFTER", "b", "X")), 6)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b101l", "2", "Y")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b101l", "1", "4")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b101l", "0", "-1")), []string{"b", "Y", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b101l", "1", "b")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b101l", "0")), "Y")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b101l2", "1", "2", "3")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("BLMOVE", "b101l2", "b101l3", "RIGHT", "LEFT", "0")), "3")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b101l2", "b101l3", "LEFT", "RIGHT")), "1")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b101l3", "0", "-1")), []string{"3", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b101lp", "x", "a", "x", "b", "x")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b101lp", "x", "MAXLEN", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b101lp", "x", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b101zlex", "0", "aa", "0", "ab", "0", "ba", "0", "bb", "0", "ca")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b101zlex", "[a", "[b")), []string{"aa", "ab"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b101zlex", "[aa", "(ba")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b101zlex", "[aa", "[ab")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b101z1", "1", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b101z2", "2", "b", "8", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b101zu", "2", "b101z1", "b101z2", "WEIGHTS", "2", "1", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b101zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "2", "d", "8", "c", "12"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b101zi", "2", "b101z1", "b101z2", "AGGREGATE", "MAX")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b101zi", "0", "-1", "WITHSCORES")), []string{"b", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b101zd", "2", "b101z1", "b101z2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b101zr", "b101z1", "0", "1")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b101b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b101b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b101b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b101b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b101ba", "b101b1", "b101b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b101ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b101bo", "b101b1", "b101b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b101bo")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b101bd", "b101b1", "b101b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b101bd", "0")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101lcs1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101lcs2", "hallo")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b101lcs1", "b101lcs2", "LEN")), 4)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b101lcs1", "b101lcs2")), "hllo")

	gs := db.Exec(nil, utils.ToCmdLine("GETSET", "b101gs", "old"))
	if _, ok := gs.(*protocol.NullBulkReply); !ok {
		t.Fatalf("GETSET miss: %T %s", gs, gs.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101gs", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b101gs", "new")), "old")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b101sx", "80", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b101sx", "b101sx2")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b101ps", "7000", "w")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b101ps")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101keep", "v", "EX", "100")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101keep", "w", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b101keep")), "w")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b101keep")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b101h", "f1", "aa", "f3", "cc")), "OK")
	hexp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b101h", "8000", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HPEXPIRE: %s", hexp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b101h1", "only", "one")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b101h1")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b101hd", "f", "v")), 1)
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b101hd", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b101s1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b101s1")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b101zpop", "1", "a")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b101zpop")), []string{"a", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b101zc", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b101zc", "2", "4")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b101zc", "0", "-1")), []string{"a", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b101dec", "3")), -3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b101dec", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b101dec", "4")), 14)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b101x", "10-0", "a", "1")), "10-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b101x", "20-0", "a", "2")), "20-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b101x", "30-0", "a", "3")), "30-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b101x", "MINID", "20-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b101x")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b101sort", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b101sort", "ALPHA", "DESC")), []string{"c", "b", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b101lx", "z")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b101lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b101lx", "b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b101sm1", "b101sm2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b101sm1", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b101sm2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b101sm1", "b101sm2", "a")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b101m1", "1", "b101m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b101m1", "x", "b101m3", "y")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b101g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b101g", "FROMMEMBER", "Palermo", "BYRADIUS", "200", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH FROMMEMBER: %s", geo.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b101")), "b101")
}
