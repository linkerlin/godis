package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2pBFLoadChunkRoundTrip(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "bf", "0.01", "100")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf", "hello")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.EXISTS", "bf", "hello")), 1)

	dump := db.Exec(nil, utils.ToCmdLine("BF.SCANDUMP", "bf", "0"))
	multi, ok := dump.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 || len(multi.Args[1]) == 0 {
		t.Fatalf("BF.SCANDUMP: %T %s", dump, dump.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DEL", "bf")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("BF.LOADCHUNK", "bf", "0", string(multi.Args[1]))), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.EXISTS", "bf", "hello")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.EXISTS", "bf", "missing")), 0)
}

func TestM2pCFReserveOptsAndLoadChunk(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CF.RESERVE", "cf", "100", "BUCKETSIZE", "2", "MAXITERATIONS", "20", "EXPANSION", "1",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "x")), 1)

	dump := db.Exec(nil, utils.ToCmdLine("CF.SCANDUMP", "cf", "0"))
	multi, ok := dump.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 || len(multi.Args[1]) == 0 {
		t.Fatalf("CF.SCANDUMP: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "cf"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CF.LOADCHUNK", "cf", "0", string(multi.Args[1]))), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.EXISTS", "cf", "x")), 1)
}

func TestM2pGeoUnitsAndAny(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"))
	r := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "g", "15", "37", "200", "mi"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GEORADIUS mi: %s", r.ToBytes())
	}
	any := db.Exec(nil, utils.ToCmdLine(
		"GEOSEARCH", "g", "FROMLONLAT", "15", "37", "BYRADIUS", "200", "km", "COUNT", "1", "ANY",
	))
	if protocol.IsErrorReply(any) {
		t.Fatalf("GEOSEARCH ANY: %s", any.ToBytes())
	}
}

func TestM2pXGroupEntriesReadAndFTAlterExplain(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "v"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XGROUP", "CREATE", "s", "g", "0-0", "ENTRIESREAD", "7",
	)), "OK")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.ALTER", "idx", "SCHEMA", "ADD", "tags", "TAG",
	)), "OK")
	ex := db.Exec(nil, utils.ToCmdLine("FT.EXPLAIN", "idx", "hello"))
	bulk, ok := ex.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "hello") {
		t.Fatalf("FT.EXPLAIN: %T %s", ex, ex.ToBytes())
	}
	cli := db.Exec(nil, utils.ToCmdLine("FT.EXPLAINCLI", "idx", "hello"))
	if _, ok := cli.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("FT.EXPLAINCLI: %T %s", cli, cli.ToBytes())
	}
}
