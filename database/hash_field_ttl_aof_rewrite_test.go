package database

import (
	"os"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestPureAofRewriteRoundTripHashFieldTTL: pure AOF rewrite (no RDB preamble)
// must emit HPEXPIREAT … FIELDS … so LoadAof restores field TTLs.
func TestPureAofRewriteRoundTripHashFieldTTL(t *testing.T) {
	tmpFile, err := os.CreateTemp(config.GetTmpDir(), "hash-fttl-*.aof")
	if err != nil {
		t.Fatal(err)
	}
	aofFilename := tmpFile.Name()
	_ = tmpFile.Close()
	defer os.Remove(aofFilename)

	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:         16,
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncAlways,
	}
	defer func() { config.Properties = old }()

	writeDB := MustNewStandaloneServer()
	conn := connection.NewFakeConn()
	asserts.AssertIntReply(t, writeDB.Exec(conn, utils.ToCmdLine("HSET", "hk", "f1", "v1", "f2", "v2")), 2)
	r := writeDB.Exec(conn, utils.ToCmdLine("HPEXPIRE", "hk", "600000", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("HPEXPIRE: %s", r.ToBytes())
	}

	ctx, err := writeDB.persister.StartRewrite()
	if err != nil {
		t.Fatalf("StartRewrite: %v", err)
	}
	if err := writeDB.persister.DoRewrite(ctx); err != nil {
		t.Fatalf("DoRewrite: %v", err)
	}
	if err := writeDB.persister.FinishRewrite(ctx); err != nil {
		t.Fatalf("FinishRewrite: %v", err)
	}
	writeDB.Close()

	readDB := MustNewStandaloneServer()
	defer readDB.Close()
	after := readDB.Exec(conn, utils.ToCmdLine("HPTTL", "hk", "FIELDS", "1", "f1"))
	assertPositiveHFieldPTTL(t, after)
	asserts.AssertBulkReply(t, readDB.Exec(conn, utils.ToCmdLine("HGET", "hk", "f1")), "v1")
	asserts.AssertBulkReply(t, readDB.Exec(conn, utils.ToCmdLine("HGET", "hk", "f2")), "v2")
	noTTL := readDB.Exec(conn, utils.ToCmdLine("HPTTL", "hk", "FIELDS", "1", "f2"))
	mr, ok := noTTL.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("f2 HPTTL: %T %s", noTTL, noTTL.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], -1)
}

func assertPositiveHFieldPTTL(t *testing.T, reply redis.Reply) {
	t.Helper()
	mr, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("HPTTL: %T %s", reply, reply.ToBytes())
	}
	ir, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok {
		t.Fatalf("HPTTL element: %T", mr.Replies[0])
	}
	if ir.Code <= 0 {
		t.Fatalf("expected positive field PTTL after AOF rewrite/load, got %d", ir.Code)
	}
}
