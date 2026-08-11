package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Gaps closed vs Redis 8.10: incomplete stream IDs (ms → ms-0), XGROUP MKSTREAM
// does not leave orphan empty streams on bad id, CREATECONSUMER missing-key ERR.

func TestGaps7XAddIncompleteStreamID(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("XADD", "xsid", "5", "f", "v"))
	asserts.AssertBulkReply(t, r, "5-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "xsid")), 1)
}

func TestGaps7XGroupCreateIncompleteID(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "xs", "1-0", "f", "v"))
	r := db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g0", "0"))
	asserts.AssertStatusReply(t, r, "OK")
}

func TestGaps7XGroupCreateMKStreamBadIDNoOrphan(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xsbad", "g", "xyz", "MKSTREAM"))
	asserts.AssertErrReply(t, r, "ERR Invalid stream ID specified as stream command argument")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "xsbad")), 0)
}

func TestGaps7XGroupCreateMKStreamIncompleteID(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xsmk", "newg", "0", "MKSTREAM"))
	asserts.AssertStatusReply(t, r, "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "xsmk")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "xsmk")), "stream")
}

func TestGaps7XGroupCreateConsumerMissingKey(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATECONSUMER", "noexist", "g", "c"))
	asserts.AssertErrReply(t, r,
		"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
}

func TestGaps7XRangeIncompleteID(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "xr", "1-0", "a", "1"))
	db.Exec(nil, utils.ToCmdLine("XADD", "xr", "2-0", "a", "2"))
	r := db.Exec(nil, utils.ToCmdLine("XRANGE", "xr", "1", "2"))
	asserts.AssertMultiBulkReplySize(t, r, 2)
}
