package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 92 R4-1 extras: LREM 0, ZUNION/ZINTER/ZDIFF, BITFIELD i8, SET XX,
// GETEX PERSIST, HPEXPIRE, ZMSCORE, LPOS RANK -1, RENAMENX fail, EXPIRE XX/NX, BITPOS 0, TYPE.
func TestR41Batch92Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b92l", "a", "b", "c", "b", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b92l", "0", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b92l")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b92z1", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b92z2", "1", "b", "3", "c")), 2)
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b92z1", "b92z2"))
	asserts.AssertMultiBulkReply(t, zu, []string{"a", "b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b92z1", "b92z2")), []string{"b"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b92z1", "b92z2")), []string{"a"})

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b92bf", "SET", "i8", "0", "-5"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bg := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b92bf", "GET", "i8", "0"))
	if protocol.IsErrorReply(bg) || !strings.Contains(string(bg.ToBytes()), "-5") {
		t.Fatalf("BITFIELD GET: %s", bg.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92xx", "v")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92xx", "w", "XX")), "OK")
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b92noxx", "w", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b92xx")), "w")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92ge", "v", "EX", "100")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b92ge", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b92ge")), -1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b92h", "f", "1.5")), "1.5")
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b92h", "5000", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(hp) {
		t.Fatalf("HPEXPIRE: %s", hp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b92zm", "1", "a", "2", "b")), 2)
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b92zm", "a", "b", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b92lp", "a", "b", "a", "c", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b92lp", "a", "RANK", "-1")), 4)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92r1", "a")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92r2", "b")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b92r1", "b92r2")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92e", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b92e", "50")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b92e", "80", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b92e2miss", "10", "NX")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b92e2", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b92e2", "10", "NX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b92bp", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b92bp", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b92bp", "0")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b92pfm", "b92pf1")), "OK")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b92tz", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b92ts", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b92th", "f", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b92tl", "x")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b92tz")), "zset")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b92ts")), "set")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b92th")), "hash")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b92tl")), "list")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ECHO", "b92")), "b92")
}
