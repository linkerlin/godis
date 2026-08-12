package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps13HGetExHSetExFieldsNumFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETEX", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hgetex' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETEX", "h", "FIELDS", "0", "f")),
		"ERR invalid number of fields")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSETEX", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hsetex' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSETEX", "h", "FIELDS", "0", "f", "v")),
		"ERR invalid number of fields")
}

func TestGaps13HGetDelFieldsNumFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETDEL", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hgetdel' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETDEL", "h", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")
}

func TestGaps13HExpireTimeFieldsNumFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HEXPIRETIME", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hexpiretime' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HEXPIRETIME", "h", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPEXPIRETIME", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hpexpiretime' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPEXPIRETIME", "h", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")
}

func TestGaps13HPersistFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPERSIST", "h", "f1")),
		"ERR wrong number of arguments for 'hpersist' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPERSIST", "h", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hpersist' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPERSIST", "h", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")

	miss := db.Exec(nil, utils.ToCmdLine("HPERSIST", "nosuch", "FIELDS", "1", "f"))
	mr, ok := miss.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("missing key: %T %s", miss, miss.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], -2)

	db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2", "c", "3"))
	db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "60", "FIELDS", "2", "a", "b"))

	r := db.Exec(nil, utils.ToCmdLine("HPERSIST", "h", "FIELDS", "3", "a", "missing", "c"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 3 {
		t.Fatalf("HPERSIST: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)  // removed TTL
	asserts.AssertIntReply(t, mr.Replies[1], -2) // missing field
	asserts.AssertIntReply(t, mr.Replies[2], -1) // no TTL
}
