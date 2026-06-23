package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	idatabase "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func newMultiDBServer(t *testing.T) *Server {
	t.Helper()
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:  4,
		AppendOnly: false,
	}
	t.Cleanup(func() { config.Properties = old })

	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	return server
}

func TestServerSelectDBAndFlush(t *testing.T) {
	server := newMultiDBServer(t)
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "db1-key", "v")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("GET", "db1-key")), "v")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "0")), "OK")
	asserts.AssertNullBulk(t, server.Exec(c, utils.ToCmdLine("GET", "db1-key")))

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FLUSHDB")), "OK")
	asserts.AssertNullBulk(t, server.Exec(c, utils.ToCmdLine("GET", "db1-key")))

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "flushall-a", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "2")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "flushall-b", "2")), "OK")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FLUSHALL")), "OK")
	asserts.AssertNullBulk(t, server.Exec(c, utils.ToCmdLine("GET", "flushall-b")))
}

func TestServerForEachAndGetEntity(t *testing.T) {
	server := newMultiDBServer(t)
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "foreach-k", "v")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("EXPIRE", "foreach-k", "3600")), 1)

	entity, ok, err := server.GetEntity(0, "foreach-k")
	if err != nil || !ok || entity == nil {
		t.Fatalf("GetEntity: ok=%v err=%v", ok, err)
	}

	exp, err := server.GetExpiration(0, "foreach-k")
	if err != nil || exp == nil {
		t.Fatalf("GetExpiration: %v", err)
	}

	count := 0
	err = server.ForEach(0, func(key string, data *idatabase.DataEntity, expiration *time.Time) bool {
		if key == "foreach-k" {
			count++
		}
		return true
	})
	if err != nil || count != 1 {
		t.Fatalf("ForEach count=%d err=%v", count, err)
	}
}

func TestServerGetDBSize(t *testing.T) {
	server := newMultiDBServer(t)
	c := connection.NewFakeConn()

	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("DBSIZE")), 0)
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "size-k1", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "size-k2", "2")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("DBSIZE")), 2)

	keys, ttlKeys, err := server.GetDBSize(0)
	if err != nil || keys != 2 {
		t.Fatalf("GetDBSize keys=%d ttl=%d err=%v", keys, ttlKeys, err)
	}
}

func TestServerSelectDBOutOfRange(t *testing.T) {
	server := newMultiDBServer(t)
	c := connection.NewFakeConn()

	reply := server.Exec(c, utils.ToCmdLine("SELECT", "99"))
	if !protocol.IsErrorReply(reply) {
		t.Fatalf("expected SELECT error, got %s", reply.ToBytes())
	}

	if _, errReply := server.selectDB(99); errReply == nil {
		t.Fatal("selectDB should fail for invalid index")
	}
}

func TestServerSlowLogLenEmpty(t *testing.T) {
	server := &Server{}
	if server.SlowLogLen() != 0 {
		t.Fatalf("expected 0 slowlog len on empty server")
	}
}

func TestServerGetLockManagerAndMemLimiter(t *testing.T) {
	server := getTestServer()
	if server.GetLockManager() == nil {
		t.Fatal("expected lock manager")
	}
	if server.GetMemLimiter() == nil {
		t.Fatal("expected memory limiter")
	}
}

func TestServerExecWithLock(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	reply := server.ExecWithLock(c, utils.ToCmdLine("SET", "lock-k", "v"))
	asserts.AssertStatusReply(t, reply, "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("GET", "lock-k")), "v")
}

func TestServerGetUndoLogs(t *testing.T) {
	server := getTestServer()
	db, _ := server.selectDB(0)

	logs, err := server.GetUndoLogs(0, utils.ToCmdLine("SET", "undo-k", "v"))
	if err != nil {
		t.Fatal(err)
	}
	_ = logs

	undo := db.GetUndoLogs(utils.ToCmdLine("SET", "undo-k2", "v2"))
	if len(undo) == 0 {
		t.Fatal("expected undo logs for SET")
	}
}

func TestServerExecMultiAndRWLocks(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "srv-tx", "1")), "QUEUED")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "srv-tx2", "2")), "QUEUED")
	execReply := server.Exec(c, utils.ToCmdLine("EXEC"))
	switch execReply.(type) {
	case *protocol.MultiBulkReply, *protocol.MultiRawReply:
	default:
		t.Fatalf("EXEC: got %T %s", execReply, execReply.ToBytes())
	}

	direct := server.ExecMulti(c, nil, []CmdLine{
		utils.ToCmdLine("SET", "srv-direct", "v"),
	})
	switch direct.(type) {
	case *protocol.MultiBulkReply, *protocol.MultiRawReply, *protocol.OkReply:
	default:
		t.Fatalf("ExecMulti: got %T %s", direct, direct.ToBytes())
	}

	if err := server.RWLocks(0, []string{"rw-w"}, []string{"rw-r"}); err != nil {
		t.Fatal(err)
	}
	if err := server.RWUnLocks(0, []string{"rw-w"}, []string{"rw-r"}); err != nil {
		t.Fatal(err)
	}
}

func TestServerKeyEventCallbacks(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	var inserted, deleted int
	server.SetKeyInsertedCallback(func(_ int, key string, _ *idatabase.DataEntity) {
		if key == "cb-key" {
			inserted++
		}
	})
	server.SetKeyDeletedCallback(func(_ int, key string, _ *idatabase.DataEntity) {
		if key == "cb-key" {
			deleted++
		}
	})

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "cb-key", "v")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("DEL", "cb-key")), 1)

	if inserted != 1 || deleted != 1 {
		t.Fatalf("callbacks inserted=%d deleted=%d", inserted, deleted)
	}
}
