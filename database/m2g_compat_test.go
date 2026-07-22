package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2gEvalCachesForEvalSha(t *testing.T) {
	db := makeTestDB()
	script := "return redis.call('GET', KEYS[1])"
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	r := db.Exec(nil, utils.ToCmdLine("EVAL", script, "1", "k"))
	asserts.AssertBulkReply(t, r, "v")

	// EVAL should have cached the script; SHA must work without SCRIPT LOAD
	sha := scriptEngine.LoadScript(script)
	r = db.Exec(nil, utils.ToCmdLine("EVALSHA", sha, "1", "k"))
	asserts.AssertBulkReply(t, r, "v")
}

func TestM2gWatchInsideMulti(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	r := db.Exec(c, utils.ToCmdLine("WATCH", "k"))
	asserts.AssertErrReply(t, r, "ERR WATCH inside MULTI is not allowed")
}

func TestM2gCMSIncrByReturnsCounts(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("CMS.INCRBY", "cms", "a", "2", "b", "3"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("CMS.INCRBY: %T %s", r, r.ToBytes())
	}
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[0]), "2")
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[1]), "3")
}

func TestM2gBFMAddDistinct(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("BF.MADD", "bf", "x", "x"))
	mr := r.(*protocol.MultiBulkReply)
	if string(mr.Args[0]) != "1" || string(mr.Args[1]) != "0" {
		t.Fatalf("BF.MADD expected 1,0 got %s,%s", mr.Args[0], mr.Args[1])
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf", "y")), 1)
}

func TestM2gJSONForgetAlias(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", ".", `{"a":1}`))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("JSON.FORGET", "j")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "j")), 0)
}

func TestM2gGeoPosMissingAndDistUnits(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("GEOPOS", "nogeo", "m1", "m2"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("GEOPOS missing key: %T %s", r, r.ToBytes())
	}
	asserts.AssertNullBulk(t, mr.Replies[0])
	asserts.AssertNullBulk(t, mr.Replies[1])

	db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo",
		"15.087269", "37.502669", "Catania"))
	r = db.Exec(nil, utils.ToCmdLine("GEODIST", "g", "Palermo", "Catania", "km"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GEODIST km: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("GEODIST", "g", "Palermo", "Catania", "mi"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GEODIST mi: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("GEODIST", "g", "Palermo", "Catania", "ft"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GEODIST ft: %s", r.ToBytes())
	}
}
