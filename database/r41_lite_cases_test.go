package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
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

	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFCOUNT", "h")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "h", "a", "b", "c")), 1)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFADD", "h", "a")), 0)
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("PFCOUNT", "h")), 3)
}
