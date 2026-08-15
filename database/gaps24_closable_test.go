package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps24GeoSearchFloatBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "g", "FROMLONLAT", "abc", "1", "BYRADIUS", "1", "m")),
		"ERR value is not a valid float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "g", "FROMLONLAT", "0", "abc", "BYRADIUS", "1", "m")),
		"ERR value is not a valid float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEOSEARCHSTORE", "out", "g", "FROMLONLAT", "abc", "1", "BYRADIUS", "1", "m")),
		"ERR value is not a valid float")
}

func TestGaps24ZUnionWeightsWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "1", "a", "WEIGHTS", "abc")),
		"ERR weight value is not a float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "d", "1", "a", "WEIGHTS", "abc")),
		"ERR weight value is not a float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "d", "1", "a", "WEIGHTS", "nan")),
		"ERR weight value is not a float")
}

func TestGaps24ConfigMaxclientsAppendonly(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxclients", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxclients", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument must be between 1 and 4294967295 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendonly", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'")
}

func TestGaps24BitCountRangeBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "nosuch", "abc", "1")),
		"ERR value is not an integer or out of range")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "nosuch", "0", "1")), 0)
}
