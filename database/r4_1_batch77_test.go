package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 77 R4-1 extras: OBJECT ENCODING, SET GET, EXISTS multi, LTRIM,
// ZCOUNT/ZINCRBY, HMSET, PEXPIREAT+PEXPIRETIME.
func TestR41Batch77Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b77str", "abc")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "b77str")), "embstr")
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b77str", "GET", "u8", "0"))
	asserts.AssertNotError(t, bf)
	if !strings.Contains(string(bf.ToBytes()), "97") {
		t.Fatalf("BITFIELD GET u8: %s", bf.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b77str", "hello", "GET")), "abc")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b77str", "b77missing")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "b77l", "a", "b", "c")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b77l", "0", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b77l")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b77l", "0")), "c")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b77z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b77z", "2", "3")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b77z", "0.5", "a")), "1.5")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b77hm", "a", "1", "b", "2")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "b77hm", "b")), "2")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b77hm")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b77pe", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIREAT", "b77pe", "2000000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRETIME", "b77pe")), 2000000000000)
}
