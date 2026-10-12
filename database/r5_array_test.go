package database

// R5-19 Array 预览子集（8.8）：断言全部取自官方命令页示例。

import (
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/datastruct/array"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestR5ARSetGetLenCount(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:1"))
	// ARSET page example.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:1", "0", "hello")), 1)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:1", "0")), "hello")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:1", "2", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:1", "2")), "a")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:1", "4")), "c")
	// Empty slot → nil.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:1", "1")))
	// Missing key / bad index → nil.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:none", "0")))
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:1", "99")))

	// ARLEN page example: sparse growth.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:2"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:2", "0", "a"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:2", "5", "b"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARLEN", "r5ar:2")), 6)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARCOUNT", "r5ar:2")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARLEN", "r5ar:none")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARCOUNT", "r5ar:none")), 0)
	// TYPE.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("TYPE", "r5ar:2")), "array")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:1", "r5ar:2"))
}

func TestR5ARMSetMGetGetRange(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:3"))
	// ARMSET page example (sparse, any order).
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"ARMSET", "r5ar:3", "0", "alpha", "5", "beta", "100", "gamma")), 3)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:3", "0")), "alpha")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:3", "5")), "beta")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:3", "100")), "gamma")

	// ARMGET page example.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:4"))
	testDB.Exec(nil, utils.ToCmdLine("ARMSET", "r5ar:4", "0", "a", "1", "b", "5", "c"))
	r := testDB.Exec(nil, utils.ToCmdLine("ARMGET", "r5ar:4", "0", "1", "5", "3"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 4 {
		t.Fatalf("ARMGET: %s", r.ToBytes())
	}
	if string(mr.Replies[0].ToBytes()) != "$1\r\na\r\n" ||
		string(mr.Replies[1].ToBytes()) != "$1\r\nb\r\n" ||
		string(mr.Replies[2].ToBytes()) != "$1\r\nc\r\n" ||
		string(mr.Replies[3].ToBytes()) != "$-1\r\n" {
		t.Errorf("ARMGET values: %s", r.ToBytes())
	}

	// ARGETRANGE page example (forward + reverse, nil for empty slots).
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:5"))
	testDB.Exec(nil, utils.ToCmdLine("ARMSET", "r5ar:5", "0", "a", "1", "b", "3", "d"))
	r = testDB.Exec(nil, utils.ToCmdLine("ARGETRANGE", "r5ar:5", "0", "3"))
	if string(r.ToBytes()) != "*4\r\n$1\r\na\r\n$1\r\nb\r\n$-1\r\n$1\r\nd\r\n" {
		t.Errorf("ARGETRANGE 0 3: %q", r.ToBytes())
	}
	r = testDB.Exec(nil, utils.ToCmdLine("ARGETRANGE", "r5ar:5", "3", "0"))
	if string(r.ToBytes()) != "*4\r\n$1\r\nd\r\n$-1\r\n$1\r\nb\r\n$1\r\na\r\n" {
		t.Errorf("ARGETRANGE 3 0: %q", r.ToBytes())
	}
	// Missing key → empty array.
	r = testDB.Exec(nil, utils.ToCmdLine("ARGETRANGE", "r5ar:none", "0", "3"))
	if string(r.ToBytes()) != "*0\r\n" {
		t.Errorf("ARGETRANGE missing key: %q", r.ToBytes())
	}
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:3", "r5ar:4", "r5ar:5"))
}

func TestR5ARDelDelRange(t *testing.T) {
	// ARDEL page example.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:6"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:6", "0", "a"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:6", "1", "b"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:6", "2", "c"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDel", "r5ar:6", "1")), 1)
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:6", "1")))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDel", "r5ar:6", "0", "2")), 2)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARCOUNT", "r5ar:6")), 0)
	// Deleting an unset index counts zero.
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDel", "r5ar:6", "1")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDel", "r5ar:none", "1")), 0)

	// ARDELRANGE page example (inclusive, overlaps once).
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:7"))
	testDB.Exec(nil, utils.ToCmdLine("ARMSET", "r5ar:7", "0", "a", "1", "b", "2", "c", "3", "d", "4", "e"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDelRange", "r5ar:7", "1", "3")), 3)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARCOUNT", "r5ar:7")), 2)
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:7", "0")), "a")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("ARGET", "r5ar:7", "4")), "e")
	// Overlapping ranges count each element once; start>end normalized.
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:8"))
	testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:8", "0", "x", "1", "y", "2", "z"))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("ARDelRange", "r5ar:8", "0", "1", "1", "2", "2", "0")), 3)
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:6", "r5ar:7", "r5ar:8"))
}

func TestR5AROpaqueRoundTrip(t *testing.T) {
	a := array.New()
	a.Set(0, []byte("a"))
	a.Set(5, []byte("b"))

	payload, ok := aof.EncodeOpaque(&database.DataEntity{Data: a})
	if !ok {
		t.Fatal("EncodeOpaque rejected the array")
	}
	entity, ok := aof.DecodeOpaque(payload)
	if !ok {
		t.Fatal("DecodeOpaque failed")
	}
	restored, ok := entity.Data.(*array.Array)
	if !ok {
		t.Fatalf("restored type %T", entity.Data)
	}
	if restored.Len() != 6 || restored.Count() != 2 {
		t.Errorf("restored len/count = %d/%d, want 6/2", restored.Len(), restored.Count())
	}
	if v, ok := restored.Get(5); !ok || string(v) != "b" {
		t.Errorf("restored slot 5 = %v", v)
	}
}

func TestR5ARWrongType(t *testing.T) {
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5ar:str", "v"))
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("ARLEN", "r5ar:str")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("ARSET", "r5ar:str", "0", "v")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5ar:str"))
}
