package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 97 R4-1 extras: LPUSHX/RPUSHX, ZADD NX/XX/CH/INCR/GT/LT, HSETNX/SETNX, SET GET,
// MSETNX, SINTER/SDIFF, ZDIFFSTORE/ZRANGESTORE, BITFIELD OVERFLOW SAT, GETDEL,
// EXPIRE NX/XX, PEXPIRE, LMOVE, GEOSEARCH FROMMEMBER, PFMERGE.
func TestR41Batch97Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b97l", "a", "b", "c", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b97l", "e")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b97l", "z")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b97l", "0", "-1")),
		[]string{"z", "a", "b", "c", "d", "e"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b97l", "b97l2", "RIGHT", "LEFT")), "e")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b97l", "0", "a")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "NX", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "NX", "1", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "XX", "2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "CH", "3", "a", "4", "b")), 2)
	zi := db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "INCR", "1", "a"))
	asserts.AssertBulkReply(t, zi, "4")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "GT", "10", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b97z", "a")), "10")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z", "LT", "5", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b97z", "a")), "5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z1", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b97z2", "1", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b97zd", "2", "b97z1", "b97z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b97zd", "0", "-1")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b97zs", "b97z1", "0", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b97z1", "a")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b97h", "f", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b97h", "f", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b97st", "v")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b97st", "v2", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b97st")), "v2")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b97a", "1", "b97b", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b97a", "9", "b97c", "3")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b97s1", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b97s2", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b97si", "b97s1", "b97s2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b97s1", "b97s2")), []string{"a"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b97s1", "b97s2")), []string{"b"})

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b97bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "100"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "100") {
		t.Fatalf("BITFIELD SAT1: %s", bf.ToBytes())
	}
	bf2 := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b97bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "100"))
	if protocol.IsErrorReply(bf2) || !strings.Contains(string(bf2.ToBytes()), "127") {
		t.Fatalf("BITFIELD SAT2: %s", bf2.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b97ex", "v", "EX", "40")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b97ex", "40", "NX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b97ex", "80", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b97ex", "9000")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b97x", "5-0", "f", "1")), "5-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b97x", "5-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b97x")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b97pfm", "b97pf")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b97pf", "x", "y")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b97pfm", "b97pf")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b97pfm")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b97g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b97g", "FROMMEMBER", "Palermo", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH: %s", geo.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b97")), "b97")
}
