package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Gaps batch 5 — verified against Redis 8.10.0 (docker :6389, requirepass ylf).

func TestGaps5HEXPIREZeroDeletesField(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hx", "f1", "v1", "f2", "v2")), 2)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hx", "-1", "FIELDS", "1", "f1")),
		"ERR invalid expire time, must be >= 0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "hx", "f1")), 1)

	r := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hx", "0", "FIELDS", "1", "f1"))
	if got := intArray(r); len(got) != 1 || got[0] != 2 {
		t.Fatalf("HEXPIRE 0 want [2], got %v (%s)", got, r.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "hx", "f1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "hx", "f2")), 1)

	r2 := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "hx", "0", "FIELDS", "1", "f2"))
	if got := intArray(r2); len(got) != 1 || got[0] != 2 {
		t.Fatalf("HPEXPIRE 0 want [2], got %v (%s)", got, r2.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "hx")), 0)
}

func TestGaps5XAddTrimLimitNeedsApprox(t *testing.T) {
	db := makeTestDB()
	msg := "ERR syntax error, LIMIT cannot be used without the special ~ option"
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"XADD", "xt", "MAXLEN", "=", "3", "LIMIT", "1", "*", "a", "1")), msg)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"XTRIM", "xt", "MINID", "=", "0-0", "LIMIT", "1")), msg)

	id := db.Exec(nil, utils.ToCmdLine("XADD", "xt0", "MAXLEN", "0", "*", "a", "1"))
	if _, ok := id.(*protocol.BulkReply); !ok {
		t.Fatalf("XADD MAXLEN 0 should return id, got %T %s", id, id.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "xt0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "xt0")), 1)
}

func TestGaps5ClusterDisabledStandalone(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("CLUSTER", "INFO")),
		"ERR This instance has cluster support disabled")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("CLUSTER", "NODES")),
		"ERR This instance has cluster support disabled")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("CLUSTER", "KEYSLOT", "foo")),
		"ERR This instance has cluster support disabled")
}
