package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
	"github.com/linkerlin/godis/scripting"
)

// FCALL_RO must reject writes from no-writes functions with the real Redis
// rejection text (bug #20: the write gate existed for EVAL_RO but was never
// armed for the Functions frontend, so no-writes functions could write).
func TestFCallROWriteGateText(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	lib := "#!lua name=wlib\nredis.register_function{function_name=\"wfn\", callback=function(keys, args) return redis.call(\"SET\", keys[1], \"hacked\") end, flags={\"no-writes\"}}\n"
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FUNCTION", "FLUSH")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("FUNCTION", "LOAD", lib)), "wlib")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "wrk", "pre")), "OK")
	r := server.Exec(c, utils.ToCmdLine("FCALL_RO", "wfn", "1", "wrk"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("FCALL_RO write should fail, got %s", r.ToBytes())
	}
	asserts.AssertErrReply(t, r, "ERR Write commands are not allowed from read-only scripts. script: wfn, on @user_function:2.")

	// The write must not have landed.
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("GET", "wrk")), "pre")
}

// EVAL_RO's rejection must name the script SHA and the raising line, byte
// identical to real Redis.
func TestEvalRORejectionNamesScript(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	script := "return redis.call(\"DEL\", KEYS[1])"
	r := server.Exec(c, utils.ToCmdLine("EVAL_RO", script, "1", "rk"))
	want := "ERR Write commands are not allowed from read-only scripts. script: " + scripting.Sha1Hex(script) + ", on @user_script:1."
	asserts.AssertErrReply(t, r, want)
}

// Nested script/function management from a Redis Function must be refused
// (real Redis behavior; also prevents scriptExecMu re-entry deadlock).
func TestFCallNestedCommandBlock(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	lib := "#!lua name=nlib\nredis.register_function{function_name=\"nested\", callback=function(keys, args) return redis.call(\"EVAL\", \"return 1\", 0) end}\n"
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FUNCTION", "FLUSH")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("FUNCTION", "LOAD", lib)), "nlib")

	r := server.Exec(c, utils.ToCmdLine("FCALL", "nested", "0"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "not allowed from script") {
		t.Fatalf("nested EVAL should be refused, got %s", r.ToBytes())
	}
}

// EVAL_RO nested read of a missing key: real Redis returns null (RESP2);
// godis leaked the Lua-false conversion as :0 (bug #21).
func TestEvalRONestedNullRead(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("EVAL_RO", `return redis.call("GET", KEYS[1])`, "1", "noread:key"))
	asserts.AssertNullBulk(t, r)
}
