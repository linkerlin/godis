package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 109 R4-1 extras: LMOVE LEFT RIGHT, LTRIM/LSET/LREM, LPOS COUNT/RANK,
// ZRANGEBYSCORE WS, ZUNIONSTORE MIN, ZINTERCARD, ZMSCORE, ZPOPMAX/BZPOPMIN,
// BITOP NOT/AND/DIFF, GETEX PERSIST, SET XX miss, HEXPIRE GT, HGETDEL,
// SDIFF/SMOVE/SINTER, XDEL, GEOSEARCH BYBOX, SORT ALPHA, LCS, PSETEX,
// PFCOUNT multi, LPUSHX.
func TestR41Batch109Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b109l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b109l", "b109l2", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b109l", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b109l", "0", "2")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b109l", "1", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b109l", "1")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b109l", "1", "X")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b109l", "0", "-1")), []string{"b", "d"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b109l")), "b")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b109l", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b109l", "0", "-1")), []string{"z", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b109lp", "a", "a", "b", "a")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b109lp", "a", "COUNT", "0")), []string{"0", "1", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b109lp", "a", "RANK", "-1")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b109z", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b109z", "(1", "4", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b109zu", "1", "b109z", "AGGREGATE", "MIN")), 4)
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b109z", "a", "missing", "d"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b109z", "b")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b109z")), []string{"d", "4"})
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b109z", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b109zu", "0", "2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b109z1", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b109z2", "5", "b", "1", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b109z1", "b109z2")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b109b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b109b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b109b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b109bn", "b109b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b109bn", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b109ba", "b109b1", "b109b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b109ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b109bd", "b109b2", "b109b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b109bd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b109b2", "1")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b109st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b109st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b109st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b109st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b109st", "!")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b109sx", "5000", "y")), "OK")
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b109xx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b109n", "2")), -2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b109sx", "3000", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b109sx")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b109h", "f1", "v1", "f2", "v2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b109h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b109h", "f3", "v3")), 1)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b109h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b109h", "80", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b109h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b109h", "f", "0.25")), "0.25")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b109sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b109sb", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b109sa", "b109sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b109sa", "b109sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b109sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b109sa", "b109sb")), []string{"b"})

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b109x", "4-0", "k", "v")), "4-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b109x", "5-0", "k", "w")), "5-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b109x", "4-0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b109g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b109g", "FROMLONLAT", "13", "38", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYBOX: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b109s1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b109s2", "hallo")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b109s1", "b109s2")), "hllo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b109s1", "b109s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b109sort", "b", "a", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b109sort", "ALPHA")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b109p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b109p2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b109p1", "b109p2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b109s1", "b109s2", "b109miss")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TOUCH", "b109s1", "b109miss")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("UNLINK", "b109miss")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p109")), "p109")
}
