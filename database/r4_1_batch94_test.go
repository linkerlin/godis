package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 94 R4-1 extras: LTRIM, ZRANGESTORE, ZINTER/ZDIFF STORE, ZLEX*, BITOP XOR/NOT,
// LCS LEN, SUBSTR/GETDEL, SET PX, HEXPIRE, PERSIST, RPOPLPUSH/LMOVE, SDIFF/SUNION STORE,
// ZPOPMAX/ZRANK, SPOP/SRANDMEMBER, HKEYS/HVALS, MSETNX, INCR/DECR, XTRIM, GEOSEARCH,
// EXPIRE GT/LT, PSETEX, LPUSHX, SMOVE, SETNX, BITFIELD INCRBY, PFMERGE, RENAME, EXPIREAT.
func TestR41Batch94Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b94l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b94l", "1", "-2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b94l", "0", "-1")), []string{"b", "c", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b94z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b94zs", "b94z", "0", "1")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b94zs", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "b", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b94zi", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b94zd", "1", "b", "3", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b94zis", "2", "b94zi", "b94zd")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b94zis", "0", "-1", "WITHSCORES")),
		[]string{"b", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b94zds", "2", "b94zi", "b94zd")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b94zds", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b94zlex", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b94zlex", "[b", "[d")),
		[]string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b94zlex", "[b", "[c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b94zlex", "[a", "[b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b94bx", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b94bd", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b94bo", "b94bx", "b94bd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b94bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b94bn", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b94bf", "b94bn")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b94bf", "0")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94lcs1", "abcdef")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94lcs2", "zbcdxy")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b94lcs1", "b94lcs2", "LEN")), 3)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94sub", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b94sub", "1", "3")), "ell")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94gd", "gone")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b94gd")), "gone")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94px", "v", "PX", "5000")), "OK")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("PTTL", "b94px")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b94hex", "f", "v")), 1)
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b94hex", "100", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE: %s", hexp.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94pe", "v", "EX", "100")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b94pe")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b94pe")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b94bl", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b94br", "x")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b94bl", "b94br")), "b")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b94bl", "b94br", "LEFT", "LEFT")), "a")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b94s1", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b94s2", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b94sd", "b94s1", "b94s2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b94su", "b94s1", "b94s2")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b94s1", "b94s2", "a")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b94zr", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b94zr", "1")), []string{"c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b94zr", "a")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b94sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b94sp")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b94sr", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b94sr")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b94h", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b94h")), []string{"f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HVALS", "b94h")), []string{"v"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b94m1", "a", "b94m2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b94m1", "x", "b94m3", "y")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b94m1", "b94m1r")), "OK")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94n", "5")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b94n")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b94n")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b94n", "10")), 15)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b94x", "1-0", "f", "1")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b94x", "2-0", "f", "2")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b94x", "3-0", "f", "3")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b94x", "MAXLEN", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b94x")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b94ge", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b94ge", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94eg", "v", "EX", "50")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b94eg", "100", "GT")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b94eg", "10", "LT")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b94pse", "8000", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b94pse")), "v")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b94lx", "z")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b94lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b94lx", "z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b94sn", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b94sn", "w")), 0)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b94bf2", "INCRBY", "u8", "0", "3"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "3") {
		t.Fatalf("BITFIELD INCRBY: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b94pfm", "b94pf1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b94pf1", "a")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b94ex", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIREAT", "b94ex", "4102444800")), 1)
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b94ex")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b94msg")), "b94msg")
}
