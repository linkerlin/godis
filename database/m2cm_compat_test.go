package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2cmUnWatchQueuedInsideMulti(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("WATCH", "k")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	r := db.Exec(c, utils.ToCmdLine("UNWATCH"))
	asserts.AssertStatusReply(t, r, "QUEUED")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("SET", "k", "1")), "QUEUED")

	exec := db.Exec(c, utils.ToCmdLine("EXEC"))
	mr, ok := exec.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("EXEC: %T %s", exec, exec.ToBytes())
	}
	if _, ok := mr.Replies[0].(*protocol.OkReply); !ok {
		t.Fatalf("UNWATCH result: %T %s", mr.Replies[0], mr.Replies[0].ToBytes())
	}
	if len(c.GetWatching()) != 0 {
		t.Fatalf("watches should be cleared after EXEC UNWATCH, got %v", c.GetWatching())
	}
}

func TestM2cmClientListWatchField(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	_ = server.Exec(c, utils.ToCmdLine("WATCH", "k"))
	r := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT LIST: %T", r)
	}
	line := string(bulk.Arg)
	if !strings.Contains(line, "watch=1") {
		t.Fatalf("want watch=1: %s", line)
	}
	if strings.Contains(line, "watching=") {
		t.Fatalf("legacy watching= should be gone: %s", line)
	}
	for _, want := range []string{"tot-net-in=", "tot-net-out=", "rbs=", "rbp="} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %s in: %s", want, line)
		}
	}
}

func TestM2cmACLGetUserFullPasswordHash(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("ACL", "SETUSER", "u1", "on", ">secret", "~*", "+@all")), "OK")

	r := db.Exec(nil, utils.ToCmdLine("ACL", "GETUSER", "u1"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("GETUSER: %T %s", r, r.ToBytes())
	}
	joined := string(r.ToBytes())
	if strings.Contains(joined, "sha256:") || strings.Contains(joined, "...") {
		t.Fatalf("truncated/prefixed hash: %s", joined)
	}
	pw, ok := m.Data["passwords"]
	if !ok || !strings.Contains(string(pw.ToBytes()), "#") {
		t.Fatalf("want #fullhash in GETUSER: %s", joined)
	}
	// full sha256 hex is 64 chars after #
	idx := strings.Index(joined, "#")
	if idx < 0 {
		t.Fatalf("no hash: %s", joined)
	}
	rest := joined[idx+1:]
	hexLen := 0
	for _, ch := range rest {
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			hexLen++
		} else {
			break
		}
	}
	if hexLen != 64 {
		t.Fatalf("hash hex len=%d want 64: %s", hexLen, joined)
	}
}
