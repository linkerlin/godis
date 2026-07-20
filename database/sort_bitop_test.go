package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestSortDescAlpha(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("RPUSH", "s:list", "10", "2", "1"))
	got := testDB.Exec(nil, utils.ToCmdLine("SORT", "s:list"))
	asserts.AssertMultiBulkReply(t, got, []string{"1", "2", "10"})

	got = testDB.Exec(nil, utils.ToCmdLine("SORT", "s:list", "DESC"))
	asserts.AssertMultiBulkReply(t, got, []string{"10", "2", "1"})

	got = testDB.Exec(nil, utils.ToCmdLine("SORT", "s:list", "ALPHA"))
	asserts.AssertMultiBulkReply(t, got, []string{"1", "10", "2"})
}

func TestSortStoreAndGetNull(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("SADD", "s:set", "a", "b"))
	testDB.Exec(nil, utils.ToCmdLine("SET", "w_a", "2"))
	testDB.Exec(nil, utils.ToCmdLine("SET", "o_a", "A"))
	n := testDB.Exec(nil, utils.ToCmdLine("SORT", "s:set", "BY", "w_*", "GET", "o_*", "GET", "#", "STORE", "s:out"))
	asserts.AssertIntReply(t, n, 4) // 2 elements * 2 GETs; b missing weight→0, missing o_b→null stored as empty
	llen := testDB.Exec(nil, utils.ToCmdLine("LLEN", "s:out"))
	asserts.AssertIntReply(t, llen, 4)
}

func TestBitOpAndOrNot(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("SET", "b:1", string([]byte{0xf0})))
	testDB.Exec(nil, utils.ToCmdLine("SET", "b:2", string([]byte{0x0f})))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b:and", "b:1", "b:2")), 1)
	got := testDB.Exec(nil, utils.ToCmdLine("GET", "b:and"))
	bulk, ok := got.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) != 1 || bulk.Arg[0] != 0x00 {
		t.Fatalf("AND: %v", got)
	}
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b:or", "b:1", "b:2")), 1)
	got = testDB.Exec(nil, utils.ToCmdLine("GET", "b:or"))
	bulk, ok = got.(*protocol.BulkReply)
	if !ok || bulk.Arg[0] != 0xff {
		t.Fatalf("OR: %v", got)
	}
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b:not", "b:1")), 1)
	got = testDB.Exec(nil, utils.ToCmdLine("GET", "b:not"))
	bulk, ok = got.(*protocol.BulkReply)
	if !ok || bulk.Arg[0] != 0x0f {
		t.Fatalf("NOT: %#v", bulk.Arg)
	}
}

func TestBitOpDiff(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("SET", "x", string([]byte{0xff})))
	testDB.Exec(nil, utils.ToCmdLine("SET", "y", string([]byte{0x0f})))
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "d", "x", "y")), 1)
	got := testDB.Exec(nil, utils.ToCmdLine("GET", "d"))
	bulk := got.(*protocol.BulkReply)
	if bulk.Arg[0] != 0xf0 {
		t.Fatalf("DIFF: %#x", bulk.Arg[0])
	}
}

func TestBitFieldGetSetIncr(t *testing.T) {
	testDB.Flush()
	r := testDB.Exec(nil, utils.ToCmdLine("BITFIELD", "bf:1", "SET", "u8", "0", "200", "INCRBY", "u8", "0", "10", "GET", "u8", "0"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 3 {
		t.Fatalf("BITFIELD: %T %v", r, r)
	}
	asserts.AssertIntReply(t, raw.Replies[0], 0)   // old value
	asserts.AssertIntReply(t, raw.Replies[1], 210) // incr
	asserts.AssertIntReply(t, raw.Replies[2], 210)
}

func TestBitFieldOverflowFail(t *testing.T) {
	testDB.Flush()
	testDB.Exec(nil, utils.ToCmdLine("BITFIELD", "bf:2", "SET", "u4", "0", "15"))
	r := testDB.Exec(nil, utils.ToCmdLine("BITFIELD", "bf:2", "OVERFLOW", "FAIL", "INCRBY", "u4", "0", "1"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 1 {
		t.Fatalf("BITFIELD FAIL: %v", r)
	}
	asserts.AssertNullBulk(t, raw.Replies[0])
}
