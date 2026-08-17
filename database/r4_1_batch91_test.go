package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 91 R4-1 extras: LPUSHX/RPUSHX, ZPOPMAX, single-member random ops,
// HKEYS/HVALS, BITOP NOT, GEO ASC COUNT 1, Z*LEX, PEXPIRE, MSET/MGET, SMOVE, XDEL.
func TestR41Batch91Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b91l", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b91l", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b91l", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b91l", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b91l", "0")), "b")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b91z", "1", "a", "2", "b", "3", "c")), 3)
	zp := db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b91z"))
	if !strings.Contains(string(zp.ToBytes()), "c") || !strings.Contains(string(zp.ToBytes()), "3") {
		t.Fatalf("ZPOPMAX: %s", zp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b91z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b91s", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b91s")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b91s")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b91s2", "x")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b91s2")), "x")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b91h", "f", "v")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b91h")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b91h")), []string{"f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HVALS", "b91h")), []string{"v"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b91bn", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b91bno", "b91bn")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "b91g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gr := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b91g", "15", "37", "200", "km", "ASC", "COUNT", "1"))
	asserts.AssertMultiBulkReply(t, gr, []string{"Catania"})
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b91g", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "ASC", "COUNT", "1"))
	asserts.AssertMultiBulkReply(t, gs, []string{"Catania"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b91zl", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b91zl", "[a", "[c")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b91zl", "[a", "[b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b91zl")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b91ea", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b91ea", "8000")), 1)
	pttl := db.Exec(nil, utils.ToCmdLine("PTTL", "b91ea"))
	if ir, ok := pttl.(*protocol.IntReply); !ok || ir.Code < 1 || ir.Code > 8000 {
		t.Fatalf("PTTL: %s", pttl.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("MSET", "b91m1", "a", "b91m2", "b")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("MGET", "b91m1", "b91m2")), []string{"a", "b"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b91sr", "0", "hello")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b91sr")), "hello")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b91db", "3")), -3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b91hd", "a", "1", "b", "2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b91hd", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b91hd")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b91sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b91sa", "b91sb", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b91sb")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b91x", "1-0", "f", "v")), "1-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b91x", "1-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b91x")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b91zs", "1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "b91zs")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b91pf", "a", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b91pf")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b91smiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b91bmiss", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b91bmiss")), 0)
}
