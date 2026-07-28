package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestM2clPubSubRESP3Push verifies SUBSCRIBE confirmations and MESSAGE
// deliveries are framed as RESP3 Push (`>`) replies for RESP3 connections,
// while RESP2 connections keep receiving classic `*` arrays.
func TestM2clPubSubRESP3Push(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()

	channel := utils.RandString(6)
	msg := utils.RandString(6)

	// RESP2 connection: subscribe confirmation is a `*` array.
	c2 := connection.NewFakeConn()
	server.Exec(c2, utils.ToCmdLine("SUBSCRIBE", channel))
	resp2Bytes := c2.Bytes()
	if len(resp2Bytes) == 0 || resp2Bytes[0] != '*' {
		t.Fatalf("RESP2 SUBSCRIBE confirmation should start with '*', got: %q", resp2Bytes)
	}

	// RESP3 connection: subscribe confirmation is a `>` push.
	c3 := connection.NewFakeConn()
	c3.SetProtocolVersion(3)
	server.Exec(c3, utils.ToCmdLine("SUBSCRIBE", channel))
	resp3Bytes := c3.Bytes()
	if len(resp3Bytes) == 0 || resp3Bytes[0] != '>' {
		t.Fatalf("RESP3 SUBSCRIBE confirmation should start with '>', got: %q", resp3Bytes)
	}

	c2.Clean()
	c3.Clean()

	server.Exec(nil, utils.ToCmdLine("PUBLISH", channel, msg))

	resp2Msg := c2.Bytes()
	if len(resp2Msg) == 0 || resp2Msg[0] != '*' {
		t.Fatalf("RESP2 MESSAGE should start with '*', got: %q", resp2Msg)
	}
	if !strings.Contains(string(resp2Msg), msg) {
		t.Fatalf("RESP2 MESSAGE missing payload: %q", resp2Msg)
	}

	resp3Msg := c3.Bytes()
	if len(resp3Msg) == 0 || resp3Msg[0] != '>' {
		t.Fatalf("RESP3 MESSAGE should start with '>', got: %q", resp3Msg)
	}
	if !strings.Contains(string(resp3Msg), msg) {
		t.Fatalf("RESP3 MESSAGE missing payload: %q", resp3Msg)
	}
}

// TestM2clPubSubRESP3UnsubscribeNothing verifies UNSUBSCRIBE with no active
// subscriptions still frames the "nothing to unsubscribe" reply as a push
// under RESP3.
func TestM2clPubSubRESP3UnsubscribeNothing(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()

	c3 := connection.NewFakeConn()
	c3.SetProtocolVersion(3)
	server.Exec(c3, utils.ToCmdLine("UNSUBSCRIBE"))
	data := c3.Bytes()
	if len(data) == 0 || data[0] != '>' {
		t.Fatalf("RESP3 UNSUBSCRIBE-nothing should start with '>', got: %q", data)
	}
	if !strings.Contains(string(data), "unsubscribe") {
		t.Fatalf("expected unsubscribe kind in reply: %q", data)
	}
}

// TestM2clLuaHKeysHValsRemainArrays confirms HKEYS/HVALS stay arrays (not
// sets) under redis.setresp(3), matching official Redis semantics.
func TestM2clLuaHKeysHValsRemainArrays(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local keys = redis.call('HKEYS', KEYS[1])
local vals = redis.call('HVALS', KEYS[1])
table.sort(keys)
table.sort(vals)
return keys[1] .. keys[2] .. vals[1] .. vals[2]
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "ab12")
}

// TestM2clLuaSScanMembersRemainArray confirms SSCAN's member list stays a
// plain array under redis.setresp(3) (SSCAN docs specify an Array reply).
func TestM2clLuaSScanMembersRemainArray(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "x", "y"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('SSCAN', KEYS[1], '0')
local members = t[2]
table.sort(members)
return members[1] .. ':' .. members[2]
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "x:y")
}

func TestM2clDebugStubs(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "SET-ACTIVE-EXPIRE", "0")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "RELOAD")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "CHANGE-REPL-ID")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "JMAP")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "FLUSHALL")), "OK")

	digest := server.Exec(c, utils.ToCmdLine("DEBUG", "DIGEST"))
	st, ok := digest.(*protocol.StatusReply)
	if !ok || len(st.Status) != 40 {
		t.Fatalf("DEBUG DIGEST want 40-char status, got %T %s", digest, digest.ToBytes())
	}

	// FLUSHALL stub must not actually remove data.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "k", "v")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "FLUSHALL")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("GET", "k")), "v")

	dv := server.Exec(c, utils.ToCmdLine("DEBUG", "DIGEST-VALUE", "k", "nosuchkey"))
	raw, ok := dv.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("DEBUG DIGEST-VALUE: %T %s", dv, dv.ToBytes())
	}
	if _, ok := raw.Replies[0].(*protocol.StatusReply); !ok {
		t.Fatalf("DEBUG DIGEST-VALUE existing key want status reply, got %T", raw.Replies[0])
	}
	if _, ok := raw.Replies[1].(*protocol.NullBulkReply); !ok {
		t.Fatalf("DEBUG DIGEST-VALUE missing key want null reply, got %T", raw.Replies[1])
	}

	match := server.Exec(c, utils.ToCmdLine("DEBUG", "STRINGMATCH-LEN", "a*", "abc"))
	asserts.AssertIntReply(t, match, 1)
	noMatch := server.Exec(c, utils.ToCmdLine("DEBUG", "STRINGMATCH-LEN", "b*", "abc"))
	asserts.AssertIntReply(t, noMatch, 0)

	help := string(server.Exec(c, utils.ToCmdLine("DEBUG", "HELP")).ToBytes())
	for _, want := range []string{"SET-ACTIVE-EXPIRE", "RELOAD", "DIGEST-VALUE", "STRINGMATCH-LEN"} {
		if !strings.Contains(help, want) {
			t.Fatalf("DEBUG HELP missing %q: %s", want, help)
		}
	}
}
