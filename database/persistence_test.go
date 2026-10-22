package database

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestLoadRDB(t *testing.T) {
	skipHeavyTests(t)
	_, b, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(b))
	os.Chdir(projectRoot)
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: "test.rdb", // set working directory to project root
	}
	// Seed test.rdb ourselves. This test used to depend on some other shuffled
	// test happening to write test.rdb via the global config leaked below, so
	// it failed on any shuffle order where it ran first.
	_ = os.Remove(filepath.Join(projectRoot, "test.rdb")) // stale leftovers
	seed := MustNewStandaloneServer()
	seedCmds := [][]string{
		{"SET", "str", "str"},
		{"RPUSH", "list", "1", "2", "3", "4"},
		{"HSET", "hash", "1", "1"},
		{"ZADD", "zset", "1", "0", "1", "1"},
		{"SADD", "set", "x"},
	}
	for _, cmd := range seedCmds {
		if r := seed.Exec(connection.NewFakeConn(), utils.ToCmdLine(cmd...)); protocol.IsErrorReply(r) {
			t.Fatalf("seed %v failed: %s", cmd, r.ToBytes())
		}
	}
	if r := SaveRDB(seed, nil); protocol.IsErrorReply(r) {
		t.Fatalf("seed SAVE failed: %s", r.ToBytes())
	}
	t.Cleanup(func() {
		_ = os.Remove(filepath.Join(projectRoot, "test.rdb"))
		config.Properties = oldProps
	})

	conn := connection.NewFakeConn()
	rdbDB := MustNewStandaloneServer()
	result := rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	asserts.AssertBulkReply(t, result, "str")
	result = rdbDB.Exec(conn, utils.ToCmdLine("LRange", "list", "0", "-1"))
	asserts.AssertMultiBulkReply(t, result, []string{"1", "2", "3", "4"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("HGetAll", "hash"))
	asserts.AssertMultiBulkReply(t, result, []string{"1", "1"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("ZRange", "zset", "0", "-1", "WITHSCORES"))
	asserts.AssertMultiBulkReply(t, result, []string{"0", "1", "1", "1"})
	result = rdbDB.Exec(conn, utils.ToCmdLine("SCard", "set"))
	asserts.AssertIntReply(t, result, 1)

	// test no rdb file
	config.Properties = &config.ServerProperties{
		AppendOnly:  false,
		RDBFilename: "noexists.rdb",
	}
	rdbDB = MustNewStandaloneServer()
	result = rdbDB.Exec(conn, utils.ToCmdLine("Get", "str"))
	asserts.AssertNullBulk(t, result)
}

func TestServerFsyncAlways(t *testing.T) {
	skipHeavyTests(t)
	aofFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	config.Properties.AppendOnly = true
	config.Properties.AppendFilename = aofFile.Name()
	config.Properties.AppendFsync = aof.FsyncAlways
	server := MustNewStandaloneServer()
	conn := connection.NewFakeConn()
	server.Exec(conn, utils.ToCmdLine("del", "1"))
	ret := server.Exec(conn, utils.ToCmdLine("incr", "1"))
	asserts.AssertNotError(t, ret)
	reader := MustNewStandaloneServer()
	ret = reader.Exec(conn, utils.ToCmdLine("get", "1"))
	asserts.AssertBulkReply(t, ret, "1")
}

func TestServerFsyncEverySec(t *testing.T) {
	skipHeavyTests(t)
	aofFile, err := ioutil.TempFile("", "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	config.Properties.AppendOnly = true
	config.Properties.AppendFilename = aofFile.Name()
	config.Properties.AppendFsync = aof.FsyncEverySec
	server := MustNewStandaloneServer()
	conn := connection.NewFakeConn()
	server.Exec(conn, utils.ToCmdLine("del", "1"))
	ret := server.Exec(conn, utils.ToCmdLine("incr", "1"))
	asserts.AssertNotError(t, ret)
	time.Sleep(1500 * time.Millisecond)
	reader := MustNewStandaloneServer()
	ret = reader.Exec(conn, utils.ToCmdLine("get", "1"))
	asserts.AssertBulkReply(t, ret, "1")
}
