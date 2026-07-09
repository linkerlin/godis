package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestStreamBasicWorkflow(t *testing.T) {
	db := makeTestDB()

	add := db.Exec(nil, utils.ToCmdLine("XADD", "s:1", "*", "field", "value"))
	idReply, ok := add.(*protocol.BulkReply)
	if !ok || len(idReply.Arg) == 0 {
		t.Fatalf("XADD: expected bulk id, got %T %s", add, add.ToBytes())
	}
	entryID := string(idReply.Arg)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s:1")), 1)

	rangeReply := db.Exec(nil, utils.ToCmdLine("XRANGE", "s:1", "-", "+"))
	multi, ok := rangeReply.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) == 0 {
		t.Fatalf("XRANGE: expected entries, got %s", rangeReply.ToBytes())
	}

	revReply := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "s:1", "+", "-", "COUNT", "1"))
	revMulti, ok := revReply.(*protocol.MultiRawReply)
	if !ok || len(revMulti.Replies) < 1 {
		t.Fatalf("XREVRANGE COUNT: got %s", revReply.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XGROUP", "CREATE", "s:1", "g1", "0-0",
	)), "OK")

	delReply := db.Exec(nil, utils.ToCmdLine("XDEL", "s:1", entryID))
	asserts.AssertIntReply(t, delReply, 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "s:1", "MAXLEN", "0")), 0)
}

func TestStreamEmptyKey(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "missing")), 0)

	empty := db.Exec(nil, utils.ToCmdLine("XRANGE", "missing", "-", "+"))
	if empty.ToBytes()[0] != '*' {
		t.Fatalf("expected empty array reply, got %s", empty.ToBytes())
	}
}

func TestStreamWrongType(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "not-stream", "x")), "OK")

	reply := db.Exec(nil, utils.ToCmdLine("XLEN", "not-stream"))
	if _, ok := reply.(*protocol.WrongTypeErrReply); !ok {
		t.Fatalf("expected WRONGTYPE, got %s", reply.ToBytes())
	}
}

func TestStreamXTrimMaxLen(t *testing.T) {
	db := makeTestDB()

	for i := 0; i < 5; i++ {
		db.Exec(nil, utils.ToCmdLine("XADD", "s:trim", "*", "n", strconv.Itoa(i)))
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s:trim")), 5)

	trim := db.Exec(nil, utils.ToCmdLine("XTRIM", "s:trim", "MAXLEN", "2"))
	if _, ok := trim.(*protocol.IntReply); !ok {
		t.Fatalf("XTRIM: expected int reply, got %s", trim.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s:trim")), 2)
}

func TestStreamXGroupHelpAndDestroy(t *testing.T) {
	db := makeTestDB()
	if _, ok := db.Exec(nil, utils.ToCmdLine("XADD", "s:g", "*", "f", "v")).(*protocol.BulkReply); !ok {
		t.Fatal("XADD should return entry id")
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:g", "grp", "0-0")), "OK")

	help := db.Exec(nil, utils.ToCmdLine("XGROUP", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "CREATE") {
		t.Fatalf("XGROUP HELP unexpected: %s", help.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "DESTROY", "s:g", "grp")), 1)
}

func TestStreamXGroupSetID(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s:setid", "*", "f", "v"))

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:setid", "g", "$")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "SETID", "s:setid", "g", "$")), "OK")
}

func TestStreamXAddOptions(t *testing.T) {
	db := makeTestDB()

	for i := 0; i < 4; i++ {
		db.Exec(nil, utils.ToCmdLine("XADD", "s:maxlen", "MAXLEN", "~", "2", "*", "n", strconv.Itoa(i)))
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s:maxlen")), 2)

	null := db.Exec(nil, utils.ToCmdLine("XADD", "missing", "NOMKSTREAM", "*", "f", "v"))
	if _, ok := null.(*protocol.NullBulkReply); !ok {
		t.Fatalf("NOMKSTREAM on missing key: got %s", null.ToBytes())
	}

	db.Exec(nil, utils.ToCmdLine("XADD", "s:minid", "*", "f", "v1"))
	if _, ok := db.Exec(nil, utils.ToCmdLine(
		"XADD", "s:minid", "MINID", "~", "0-0", "*", "f", "v2",
	)).(*protocol.BulkReply); !ok {
		t.Fatal("XADD MINID should return entry id")
	}
}
