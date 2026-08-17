package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 87 R4-1 extras: miss nils, SET GET, BITFIELD SET, float create, LMOVE/LPOS, ZREMRANGEBYSCORE, SMISMEMBER.
func TestR41Batch87Extras(t *testing.T) {
	db := makeTestDB()

	if _, ok := db.Exec(nil, utils.ToCmdLine("ZSCORE", "b87zmiss", "m")).(*protocol.NullBulkReply); !ok {
		t.Fatal("ZSCORE miss")
	}
	if _, ok := db.Exec(nil, utils.ToCmdLine("ZRANK", "b87zmiss", "m")).(*protocol.NullBulkReply); !ok {
		t.Fatal("ZRANK miss")
	}
	if _, ok := db.Exec(nil, utils.ToCmdLine("GETDEL", "b87gdmiss")).(*protocol.NullBulkReply); !ok {
		t.Fatal("GETDEL miss")
	}
	if _, ok := db.Exec(nil, utils.ToCmdLine("LMOVE", "b87lmiss", "b87lmiss2", "LEFT", "LEFT")).(*protocol.NullBulkReply); !ok {
		t.Fatal("LMOVE miss")
	}
	if _, ok := db.Exec(nil, utils.ToCmdLine("LPOS", "b87lmiss", "x")).(*protocol.NullBulkReply); !ok {
		t.Fatal("LPOS miss")
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b87smiss")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b87grmiss", "0", "-1")), "")

	idle := db.Exec(nil, utils.ToCmdLine("OBJECT", "IDLETIME", "b87idlemiss"))
	if !protocol.IsErrorReply(idle) {
		if _, ok := idle.(*protocol.NullBulkReply); !ok {
			// Redis 8 may return null; some paths IntReply -2-like miss.
			if ir, ok := idle.(*protocol.IntReply); ok && ir.Code != 0 {
				t.Fatalf("OBJECT IDLETIME miss: %s", idle.ToBytes())
			}
		}
	}

	sg := db.Exec(nil, utils.ToCmdLine("SET", "b87sg", "v", "GET"))
	if _, ok := sg.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET GET create: %T %s", sg, sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b87sg", "w", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b87sg")), "w")

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b87bf", "SET", "u8", "0", "7"))
	asserts.AssertNotError(t, bf)
	got := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b87bf", "GET", "u8", "0"))
	if protocol.IsErrorReply(got) {
		t.Fatalf("BITFIELD GET: %s", got.ToBytes())
	}
	if !strings.Contains(string(got.ToBytes()), "7") {
		t.Fatalf("BITFIELD GET want 7: %s", got.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b87hf", "f", "0.5")), "0.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b87ib", "2.25")), "2.25")

	if _, ok := db.Exec(nil, utils.ToCmdLine("GETSET", "b87gs", "old")).(*protocol.NullBulkReply); !ok {
		t.Fatal("GETSET create")
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b87gs", "new")), "old")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b87sr", "0", "hi!")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b87sr")), "hi!")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "b87l", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b87l", "b")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b87l", "b87l2", "LEFT", "RIGHT")), "c")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b87l")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b87l2")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b87z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b87z", "1", "2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b87z")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b87s", "x", "y", "z")), 3)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b87s", "x", "q", "y"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "1") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	if !strings.Contains(string(sm.ToBytes()), ":0") && !strings.Contains(string(sm.ToBytes()), "\n0\n") {
		// RESP array of ints 1,0,1
		if !strings.Contains(string(sm.ToBytes()), "0") {
			t.Fatalf("SMISMEMBER missing 0: %s", sm.ToBytes())
		}
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b87x", "1-0", "f", "v")), "1-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b87x", "MAXLEN", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b87x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b87db", "5")), -5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b87ttlmiss")), -2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b87typemiss")), "none")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b87emiss")), 0)
}
