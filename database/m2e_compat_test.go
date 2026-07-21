package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2eHScanMissingAndCursor(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("HSCAN", "nosuch", "0"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("missing key HSCAN: %T %s", r, r.ToBytes())
	}
	asserts.AssertBulkReply(t, mr.Replies[0], "0")
	asserts.AssertMultiBulkReplySize(t, mr.Replies[1], 0)

	db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4"))
	seen := map[string]string{}
	cursor := "0"
	for i := 0; i < 20; i++ {
		r = db.Exec(nil, utils.ToCmdLine("HSCAN", "h", cursor, "COUNT", "1"))
		mr = r.(*protocol.MultiRawReply)
		cursor = string(mr.Replies[0].(*protocol.BulkReply).Arg)
		arr := mr.Replies[1].(*protocol.MultiBulkReply)
		for j := 0; j+1 < len(arr.Args); j += 2 {
			seen[string(arr.Args[j])] = string(arr.Args[j+1])
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 4 {
		t.Fatalf("HSCAN should visit all fields, got %v", seen)
	}
}

func TestM2eDumpRestoreTypesAndCRC(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "hello")), "OK")
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "s")).(*protocol.BulkReply)
	if len(dump.Arg) < 10 {
		t.Fatalf("DUMP too short: %d", len(dump.Arg))
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "s"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "s", "0", string(dump.Arg))), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "s")), "hello")

	// corrupt CRC
	bad := append([]byte{}, dump.Arg...)
	bad[len(bad)-1] ^= 0xff
	r := db.Exec(nil, utils.ToCmdLine("RESTORE", "bad", "0", string(bad)))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected CRC error, got %s", r.ToBytes())
	}

	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	hdump := db.Exec(nil, utils.ToCmdLine("DUMP", "h")).(*protocol.BulkReply)
	db.Exec(nil, utils.ToCmdLine("DEL", "h"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "h2", "0", string(hdump.Arg))), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "h2", "f")), "v")

	db.Exec(nil, utils.ToCmdLine("SADD", "set", "m1", "m2"))
	sdump := db.Exec(nil, utils.ToCmdLine("DUMP", "set")).(*protocol.BulkReply)
	db.Exec(nil, utils.ToCmdLine("DEL", "set"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "set2", "0", string(sdump.Arg))), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "set2")), 2)

	// RESTORE ttl=0 clears previous TTL
	db.Exec(nil, utils.ToCmdLine("SET", "ttl", "x", "EX", "100"))
	tdump := db.Exec(nil, utils.ToCmdLine("DUMP", "ttl")).(*protocol.BulkReply)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE", "ttl", "0", string(tdump.Arg), "REPLACE",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "ttl")), -1)
}

func TestM2eKeysGlobNoRegexInjection(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "a+b", "1"))
	db.Exec(nil, utils.ToCmdLine("SET", "ab", "1"))
	db.Exec(nil, utils.ToCmdLine("SET", "axb", "1"))

	// '+' is literal in glob, not "one or more"
	r := db.Exec(nil, utils.ToCmdLine("KEYS", "a+b"))
	asserts.AssertMultiBulkReplySize(t, r, 1)

	r = db.Exec(nil, utils.ToCmdLine("KEYS", "a?b"))
	mr := r.(*protocol.MultiBulkReply)
	found := map[string]bool{}
	for _, a := range mr.Args {
		found[string(a)] = true
	}
	if !found["a+b"] || !found["axb"] || found["ab"] {
		t.Fatalf("KEYS a?b got %v", found)
	}

	p, err := wildcard.CompilePattern("a+b")
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsMatch("a+b") || p.IsMatch("ab") || p.IsMatch("axb") {
		t.Fatal("glob '+' must be literal")
	}
	if p.IsMatch("aab") {
		t.Fatal("+ must not mean regex quantifier")
	}
}
