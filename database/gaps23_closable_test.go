package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps23BFRateRange(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "b", "-0.1", "100")),
		"ERR error rate must be in the range (0.000000, 1.000000)")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "b2", "2", "100")),
		"ERR error rate must be in the range (0.000000, 1.000000)")
}

func TestGaps23CFCapacityRange(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "c", "0")),
		"Capacity must be in the range [2 * BUCKETSIZE, 1073741824]")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "c2", "-1")),
		"Capacity must be in the range [2 * BUCKETSIZE, 1073741824]")
}

func TestGaps23TopKAndCMSAndTDigest(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "t", "0")),
		"TopK: invalid k")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYPROB", "c", "abc", "0.01")),
		"CMS: invalid overestimation value")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYPROB", "c2", "0.01", "2")),
		"CMS: invalid prob value")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td", "COMPRESSION", "-1")),
		"ERR T-Digest: compression parameter needs to be a positive integer")
}

func TestGaps23TSNegative(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATE", "t", "RETENTION", "-1")),
		"ERR TSDB: Couldn't parse RETENTION")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.ADD", "t2", "-1", "1")),
		"ERR TSDB: invalid timestamp, must be a nonnegative integer")
}

func TestGaps23XAddZeroAndXGroupDestroy(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "0-0", "f", "v")),
		"ERR The ID specified in XADD must be greater than 0-0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "0", "f", "v")),
		"ERR The ID specified in XADD must be greater than 0-0")
	db.Exec(nil, utils.ToCmdLine("XADD", "x2", "1-0", "f", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "x2", "1-0", "f", "v")),
		"ERR The ID specified in XADD is equal or smaller than the target stream top item")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "DESTROY", "nosuch", "g")),
		"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
}

func TestGaps23JSONMissAndEOF(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", "$", "{")),
		"EOF while parsing an object at line 1 column 1")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRAPPEND", "nosuch", "$", "1")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.NUMINCRBY", "nosuch", "$", "1")),
		"ERR could not perform this operation on a key that doesn't exist")
}

func TestGaps23ConfigSaveAndGeoRadius(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "save", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "save", "")), "OK")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "save", "0 0")),
		"ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters")

	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEORADIUS", "g", "0", "0", "abc", "m")),
		"ERR need numeric radius")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEORADIUSBYMEMBER", "g", "m", "abc", "m")),
		"ERR need numeric radius")
}

func TestGaps23FunctionLoadRestoreWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", "xx")),
		"ERR Missing library metadata")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", "#!lua")),
		"ERR Invalid library metadata")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", "xx", "0")),
		"ERR Wrong restore policy given, value should be either FLUSH, APPEND or REPLACE.")
}
