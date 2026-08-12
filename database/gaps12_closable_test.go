package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps12ClientCachingOptInOptOut(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetProtocolVersion(3)

	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("YES")}),
		"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled")
	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("NO")}),
		"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled")

	idDefault := EnableTracking(c, "", nil, "", false)
	c.SetTrackingID(idDefault)
	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("YES")}),
		"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.")
	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("NO")}),
		"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.")
	DisableTracking(idDefault)
	c.SetTrackingID("")

	idOptIn := EnableTracking(c, "optin", nil, "", false)
	c.SetTrackingID(idOptIn)
	asserts.AssertStatusReply(t, execClientCachingConn(c, [][]byte{[]byte("YES")}), "OK")
	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("NO")}),
		"ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.")
	DisableTracking(idOptIn)
	c.SetTrackingID("")

	idOptOut := EnableTracking(c, "optout", nil, "", false)
	c.SetTrackingID(idOptOut)
	defer DisableTracking(idOptOut)
	asserts.AssertStatusReply(t, execClientCachingConn(c, [][]byte{[]byte("NO")}), "OK")
	asserts.AssertErrReply(t, execClientCachingConn(c, [][]byte{[]byte("YES")}),
		"ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.")
}

func TestGaps12HTTLFieldsNumFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HTTL", "miss", "FIELDS", "0")),
		"ERR wrong number of arguments for 'httl' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HTTL", "miss", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPTTL", "miss", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hpttl' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPTTL", "miss", "FIELDS", "0", "f")),
		"ERR Number of fields must be a positive integer")

	db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1"))
	db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "60", "FIELDS", "1", "a"))
	r := db.Exec(nil, utils.ToCmdLine("HTTL", "h", "FIELDS", "1", "a"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("HTTL FIELDS: %T %s", r, r.ToBytes())
	}
	ttl, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || ttl.Code <= 0 {
		t.Fatalf("expected positive HTTL, got %T %s", mr.Replies[0], mr.Replies[0].ToBytes())
	}
}
