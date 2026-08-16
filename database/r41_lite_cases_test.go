package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Locks R4-1 lite expected replies used by scripts/r4-1-cases.txt (Godis side).
func TestR41LiteCaseExpectations(t *testing.T) {
	testDB.Flush()
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("XADD", "s", "1-0", "f", "v")), "1-0")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "s")), 1)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("XADD", "s", "2-0", "f", "w")), "2-0")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("XLEN", "s")), 2)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TYPE", "s")), "stream")

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZCARD", "g")), 2)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TYPE", "g")), "zset")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo")), 0)

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SETBIT", "b", "7", "1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("GETBIT", "b", "7")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SETBIT", "b", "0", "1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "b")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SETBIT", "b2", "0", "1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "AND", "band", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "band")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "OR", "bor", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bor")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "bxor", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bxor")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "bnot", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bnot")), 7)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "bdiff", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bdiff")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "DIFF1", "bd1", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bd1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "bao", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bao")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "bone", "b", "b2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITCOUNT", "bone")), 1)

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFCOUNT", "h")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "h", "a", "b", "c")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "h", "a")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFCOUNT", "h")), 3)

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "lcs1", "ohmytext")), "OK")
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "lcs2", "mynewtext")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LCS", "lcs1", "lcs2")), "mytext")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("MSETNX", "mn1", "1", "mn2", "2")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("MSETNX", "mn1", "3", "mn3", "4")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("LPUSHX", "lpx", "x")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("LPUSH", "lpx", "a")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("LPUSHX", "lpx", "b")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSHX", "rpx", "x")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "rpx", "a")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSHX", "rpx", "b")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "zr", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZREVRANK", "zr", "c")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SETBIT", "bp", "7", "1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITPOS", "bp", "1")), 7)

	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "ibf", "1.5")), "1.5")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("TOUCH", "missing-touch")), 0)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "touch-k", "v")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("TOUCH", "touch-k")), 1)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("RENAME", "touch-k", "renamed-k")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "touch-k")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "renamed-k")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXPIREAT", "renamed-k", "2000000000")), 1)

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "gx", "v", "EX", "100")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GETEX", "gx", "PERSIST")), "v")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("TTL", "gx")), -1)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "et", "v", "EXAT", "2000000000")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXPIRETIME", "et")), 2000000000)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "lp", "a", "b", "a", "c", "a")), 5)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("LPOS", "lp", "a")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("LPOS", "lp", "a", "RANK", "2")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "zs", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "zs", "1", "2")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZCARD", "zs")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "lm", "a", "b")), 2)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LMOVE", "lm", "lm2", "LEFT", "RIGHT")), "a")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LINDEX", "lm2", "0")), "a")
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("PSETEX", "ps", "120000", "val")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", "ps")), "val")

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b61z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZRANGE", "b61z", "0", "0")), []string{"a"})
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZSCORE", "b61z", "b")), "2")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZRANK", "b61z", "a")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("HSET", "b61h", "f1", "v1", "f2", "v2")), 2)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("HMGET", "b61h", "f1")), []string{"v1"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("HSTRLEN", "b61h", "f1")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("RPUSH", "b61l", "x", "y", "z")), 3)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("LRANGE", "b61l", "0", "0")), []string{"x"})
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "b61gd", "bye")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GETDEL", "b61gd")), "bye")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("EXISTS", "b61gd")), 0)

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b62z", "1", "a", "2", "b")), 2)
	zm := testDB.Exec(nil, utils.ToCmdLine("ZMSCORE", "b62z", "a"))
	if mr, ok := zm.(*protocol.MultiRawReply); !ok || len(mr.Replies) != 1 {
		t.Fatalf("ZMSCORE type %T %s", zm, zm.ToBytes())
	} else if d, ok := mr.Replies[0].(*protocol.DoubleReply); !ok || d.Value != 1 {
		t.Fatalf("ZMSCORE[0] want Double 1, got %T %s", mr.Replies[0], zm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b62z", "0", "0")), []string{"b"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("SADD", "b62s", "a", "b")), 2)
	sm := testDB.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b62s", "a"))
	if mr, ok := sm.(*protocol.MultiRawReply); !ok || len(mr.Replies) != 1 {
		t.Fatalf("SMISMEMBER type %T %s", sm, sm.ToBytes())
	} else {
		asserts.AssertIntReply(t, mr.Replies[0], 1)
	}
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b62h", "f", "0.5")), "0.5")
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("SET", "b62pe", "v", "PXAT", "2000000000000")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PEXPIRETIME", "b62pe")), 2000000000000)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b62d1", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b62d2", "3", "b")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b62do", "2", "b62d1", "b62d2")), 1)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZSCORE", "b62do", "a")), "1")

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b63zp", "1", "a", "2", "b")), 2)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b63zp")), []string{"a", "1"})
	// After ZPOPMIN, member b remains → ZADD only counts new member a.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b63zp", "1", "a", "2", "b")), 1)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b63zp")), []string{"b", "2"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b63zl", "0", "a", "0", "b", "0", "c")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b63zl", "-", "+")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b63zl", "[a", "[b")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "b63pf1", "a")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "b63pf2", "b")), 1)
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("PFMERGE", "b63pfo", "b63pf1", "b63pf2")), "OK")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFCOUNT", "b63pfo")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b63zr", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b63zr", "1", "1")), []string{"a"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZCOUNT", "b63zr", "1", "2")), 2)

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZADD", "b65zl", "0", "a", "0", "b", "0", "c")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b65zl", "[a", "[b")), 2)
	asserts.AssertMultiBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b65zl", "-", "+")), []string{"c"})
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("HSET", "b65h", "f", "v")), 1)
	hpe := testDB.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b65h", "100000", "FIELDS", "1", "f"))
	if mr, ok := hpe.(*protocol.MultiRawReply); !ok || len(mr.Replies) != 1 {
		t.Fatalf("HPEXPIRE type %T %s", hpe, hpe.ToBytes())
	} else {
		asserts.AssertIntReply(t, mr.Replies[0], 1)
	}
	httl := testDB.Exec(nil, utils.ToCmdLine("HPTTL", "b65h", "FIELDS", "1", "f"))
	if mr, ok := httl.(*protocol.MultiRawReply); !ok || len(mr.Replies) != 1 {
		t.Fatalf("HPTTL type %T %s", httl, httl.ToBytes())
	} else {
		ir, ok := mr.Replies[0].(*protocol.IntReply)
		if !ok || ir.Code < 1 || ir.Code > 100000 {
			t.Fatalf("HPTTL want 1..100000, got %T %s", mr.Replies[0], httl.ToBytes())
		}
	}
}
