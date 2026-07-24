package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2abGeoSearchIncludesSelf(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "13.361389", "38.115556", "Palermo",
	)), 1)
	r := db.Exec(nil, utils.ToCmdLine(
		"GEOSEARCH", "g", "FROMMEMBER", "Palermo", "BYRADIUS", "1", "km",
	))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("GEOSEARCH: %s", r.ToBytes())
	}
	asserts.AssertBulkReply(t, mr.Replies[0], "Palermo")
}

func TestM2abGeoHashMissingKey(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("GEOHASH", "nokey", "a", "b"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 || mr.Args[0] != nil || mr.Args[1] != nil {
		t.Fatalf("GEOHASH missing key: %s", r.ToBytes())
	}
}

func TestM2abGeoRadiusMissingKey(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "nokey", "0", "0", "1", "km"))
	asserts.AssertMultiBulkReplySize(t, r, 0)
}

func TestM2abXReadGroupDeliveryCount(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "1"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XGROUP", "CREATE", "s", "g", "0-0",
	)), "OK")
	c := connection.NewFakeConn()
	db.Exec(c, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">",
	))
	pending := db.Exec(nil, utils.ToCmdLine("XPENDING", "s", "g", "-", "+", "10", "c1"))
	count1 := pendingDeliveryCount(t, pending)
	if count1 != "1" {
		t.Fatalf("first delivery count want 1 got %q reply=%s", count1, pending.ToBytes())
	}

	db.Exec(c, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", "0-0",
	))
	pending2 := db.Exec(nil, utils.ToCmdLine("XPENDING", "s", "g", "-", "+", "10", "c1"))
	count2 := pendingDeliveryCount(t, pending2)
	if count2 != "2" {
		t.Fatalf("reread delivery count want 2 got %q", count2)
	}
}

func pendingDeliveryCount(t *testing.T, pending redisReply) string {
	t.Helper()
	switch r := pending.(type) {
	case *protocol.MultiRawReply:
		if len(r.Replies) < 1 {
			t.Fatalf("XPENDING empty: %s", pending.ToBytes())
		}
		row, ok := r.Replies[0].(*protocol.MultiBulkReply)
		if !ok || len(row.Args) < 4 {
			t.Fatalf("pending row: %s", pending.ToBytes())
		}
		return string(row.Args[3])
	case *protocol.MultiBulkReply:
		if len(r.Args) < 1 {
			t.Fatalf("XPENDING empty multibulk: %s", pending.ToBytes())
		}
		// Nested RESP bytes in Args[0] — prefer MultiRaw path above.
		t.Fatalf("unexpected MultiBulk XPENDING: %s", pending.ToBytes())
	default:
		t.Fatalf("XPENDING type %T: %s", pending, pending.ToBytes())
	}
	return ""
}

// redisReply avoids importing interface/redis just for helper signature clarity.
type redisReply = interface {
	ToBytes() []byte
}

func TestM2abReplConfCapa(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"REPLCONF", "listening-port", "6380",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"REPLCONF", "ip-address", "127.0.0.1",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"REPLCONF", "capa", "psync2",
	)), "OK")
}
