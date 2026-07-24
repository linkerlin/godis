package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2zClientInfoAgeIdle(t *testing.T) {
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)
	c.SetClientTimesForTest(time.Now().Add(-5*time.Second), time.Now().Add(-2*time.Second))
	r := execClientInfoConn(c, nil)
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT INFO: %T", r)
	}
	line := string(bulk.Arg)
	if !strings.Contains(line, "age=") || strings.Contains(line, "age=0 ") {
		t.Fatalf("expected non-zero age: %s", line)
	}
	if !strings.Contains(line, "idle=") || strings.Contains(line, "idle=0 ") {
		t.Fatalf("expected non-zero idle: %s", line)
	}
}

func TestM2zXDelexPerIDArray(t *testing.T) {
	db := makeTestDB()
	id1 := db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "1")).(*protocol.BulkReply)
	id2 := db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "2")).(*protocol.BulkReply)
	r := db.Exec(nil, utils.ToCmdLine(
		"XDELEX", "s", "IDS", "3", string(id1.Arg), string(id2.Arg), "0-1",
	))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 3 {
		t.Fatalf("XDELEX per-id: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)
	asserts.AssertIntReply(t, mr.Replies[1], 1)
	asserts.AssertIntReply(t, mr.Replies[2], -1)
}

func TestM2zUndoHGetExFields(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "old"))
	undo := undoHGetEx(db, utils.ToCmdLine("HGETEX", "h", "EX", "60", "FIELDS", "1", "a")[1:])
	if len(undo) < 1 {
		t.Fatalf("expected undo cmds, got %#v", undo)
	}
	foundHSet := false
	for _, cmd := range undo {
		if len(cmd) >= 4 && strings.EqualFold(string(cmd[0]), "HSET") &&
			string(cmd[2]) == "a" && string(cmd[3]) == "old" {
			foundHSet = true
		}
	}
	if !foundHSet {
		t.Fatalf("undo missing HSET restore: %#v", undo)
	}
}

func TestM2zJSONUndoRegistered(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", "$", `{"a":1}`)), "OK")
	undo := undoJSONKey(db, [][]byte{[]byte("j")})
	if len(undo) < 2 {
		t.Fatalf("expected DEL+restore undo, got %#v", undo)
	}
	if !strings.EqualFold(string(undo[0][0]), "DEL") {
		t.Fatalf("first undo should DEL: %#v", undo[0])
	}
	// Apply undo after mutation to verify restore path.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", "$", `{"a":99}`)), "OK")
	for _, cmd := range undo {
		db.Exec(nil, cmd)
	}
	got := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", "$"))
	bulk, ok := got.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), `"a":1`) {
		t.Fatalf("after undo expected a=1, got %s", got.ToBytes())
	}
}

func TestM2zFCallGopherLua(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	code := "#!lua name=m2zlib api_version=1.0\n" +
		"redis.register_function('m2z_get', function(keys, args) return redis.call('GET', keys[1]) end)"
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code))
	if protocol.IsErrorReply(r) {
		t.Fatalf("FUNCTION LOAD: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "k", "hello")), "OK")
	got := db.Exec(nil, utils.ToCmdLine("FCALL", "m2z_get", "1", "k"))
	asserts.AssertBulkReply(t, got, "hello")
}
