package database

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/linkerlin/godis/aof"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func makeTestData(db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewFakeConn()
	conn.SelectDB(dbIndex)
	db.Exec(conn, utils.ToCmdLine("FlushDB"))
	cursor := 0
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SET", key, key, "EX", "10000"))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("RPUSH", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("HSET", key, key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("SADD", key, key))
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		db.Exec(conn, utils.ToCmdLine("ZADD", key, "10", key))
	}
}

func validateTestData(t *testing.T, db database.DB, dbIndex int, prefix string, size int) {
	conn := connection.NewFakeConn()
	conn.SelectDB(dbIndex)
	cursor := 0
	var ret redis.Reply
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("GET", key))
		asserts.AssertBulkReply(t, ret, key)
		ret = db.Exec(conn, utils.ToCmdLine("TTL", key))
		intResult, ok := ret.(*protocol.IntReply)
		if !ok {
			t.Errorf("expected int protocol, actually %s", ret.ToBytes())
			return
		}
		if intResult.Code <= 0 || intResult.Code > 10000 {
			t.Error("wrong ttl")
		}
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("LRANGE", key, "0", "-1"))
		asserts.AssertMultiBulkReply(t, ret, []string{key})
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("HGET", key, key))
		asserts.AssertBulkReply(t, ret, key)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("SIsMember", key, key))
		asserts.AssertIntReply(t, ret, 1)
	}
	for i := 0; i < size; i++ {
		key := prefix + strconv.Itoa(cursor)
		cursor++
		ret = db.Exec(conn, utils.ToCmdLine("ZRANGE", key, "0", "-1"))
		asserts.AssertMultiBulkReply(t, ret, []string{key})
	}
}

func TestAof(t *testing.T) {
	skipHeavyTests(t)
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncEverySec,
	}
	dbNum := 1
	size := 3
	var prefixes []string
	aofWriteDB := MustNewStandaloneServer()
	// generate test data
	for i := 0; i < dbNum; i++ {
		prefix := utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(aofWriteDB, i, prefix, size)
	}
	aofWriteDB.Close()                     // wait for aof finished
	aofReadDB := MustNewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, aofReadDB, i, prefix, size)
	}
	aofReadDB.Close()
}

func TestRDB(t *testing.T) {
	skipHeavyTests(t)
	tmpDir, err := ioutil.TempDir("", "godis")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := path.Join(tmpDir, "a.aof")
	rdbFilename := path.Join(tmpDir, "dump.rdb")
	defer func() {
		_ = os.Remove(aofFilename)
		_ = os.Remove(rdbFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
		RDBFilename:    rdbFilename,
	}
	dbNum := 1
	size := 3
	var prefixes []string
	conn := connection.NewFakeConn()
	writeDB := MustNewStandaloneServer()
	for i := 0; i < dbNum; i++ {
		prefix := utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(writeDB, i, prefix, size)
	}
	time.Sleep(time.Second) // wait for aof finished
	writeDB.Exec(conn, utils.ToCmdLine("save"))
	writeDB.Close()
	readDB := MustNewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, readDB, i, prefix, size)
	}
	readDB.Close()
}

func TestConcurrentBackgroundRewriteRejected(t *testing.T) {
	skipHeavyTests(t)
	tmpFile, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		t.Fatal(err)
	}
	aofFilename := tmpFile.Name()
	defer os.Remove(aofFilename)

	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncEverySec,
	}
	db := MustNewStandaloneServer()
	defer db.Close()

	if err := db.persister.RunRewriteAsync(); err != nil {
		t.Fatalf("first RunRewriteAsync: %v", err)
	}
	if err := db.persister.RunRewriteAsync(); err != aof.ErrRewriteInProgress {
		t.Fatalf("second RunRewriteAsync = %v, want ErrRewriteInProgress", err)
	}

	ret := db.Exec(nil, utils.ToCmdLine("BGREWRITEAOF"))
	asserts.AssertErrReply(t, ret, "ERR Background rewrite already in progress")

	rdbName := path.Join(config.GetTmpDir(), "concurrent-save.rdb")
	defer os.Remove(rdbName)
	ret = db.Exec(nil, utils.ToCmdLine("BGSAVE"))
	asserts.AssertErrReply(t, ret, "ERR Background rewrite already in progress")

	time.Sleep(2 * time.Second)
}

func TestRewriteAOF(t *testing.T) {
	skipHeavyTests(t)
	tmpFile, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := tmpFile.Name()
	defer func() {
		_ = os.Remove(aofFilename)
	}()
	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncEverySec,
	}
	aofWriteDB := MustNewStandaloneServer()
	size := 1
	dbNum := 1
	var prefixes []string
	for i := 0; i < dbNum; i++ {
		prefix := "" // utils.RandString(8)
		prefixes = append(prefixes, prefix)
		makeTestData(aofWriteDB, i, prefix, size)
	}
	//time.Sleep(2 * time.Second)
	aofWriteDB.Exec(nil, utils.ToCmdLine("rewriteaof"))
	time.Sleep(2 * time.Second)            // wait for async goroutine finish its job
	aofWriteDB.Close()                     // wait for aof finished
	aofReadDB := MustNewStandaloneServer() // start new db and read aof file
	for i := 0; i < dbNum; i++ {
		prefix := prefixes[i]
		validateTestData(t, aofReadDB, i, prefix, size)
	}
	aofReadDB.Close()
}

// TestRewriteAOF2 tests execute commands during rewrite procedure
func TestRewriteAOF2(t *testing.T) {
	skipHeavyTests(t)
	/* prepare */
	tmpFile, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		t.Error(err)
		return
	}
	aofFilename := tmpFile.Name()
	config.Properties = &config.ServerProperties{
		AppendOnly:     true,
		AppendFilename: aofFilename,
		// set Aof-use-rdb-preamble to true to make sure rewrite procedure
		AppendFsync:       aof.FsyncAlways,
		AofUseRdbPreamble: true,
	}

	keySize1 := 5
	keySize2 := 10
	/* write data */
	aofWriteDB := MustNewStandaloneServer()
	dbNum := 1
	conn := connection.NewFakeConn()
	for i := 0; i < dbNum; i++ {
		conn.SelectDB(i)
		for j := 0; j < keySize1; j++ {
			key := strconv.Itoa(j)
			aofWriteDB.Exec(conn, utils.ToCmdLine("SET", key, key))
		}
	}

	/* rewrite */
	ctx, err := aofWriteDB.persister.StartRewrite()
	if err != nil {
		t.Error(err, "start rewrite failed")
		return
	}

	/* add data during rewrite */
	ch := make(chan struct{})
	go func() {
		for i := 0; i < dbNum; i++ {
			conn.SelectDB(i)
			for j := 0; j < keySize2; j++ {
				key := "a" + strconv.Itoa(j)
				aofWriteDB.Exec(conn, utils.ToCmdLine("SET", key, key))
			}
		}
		ch <- struct{}{}
	}()

	doRewriteErr := aofWriteDB.persister.DoRewrite(ctx)
	if doRewriteErr != nil {
		t.Error(doRewriteErr, "do rewrite failed")
		return
	}
	if err := aofWriteDB.persister.FinishRewrite(ctx); err != nil {
		t.Error(err)
		return
	}
	<-ch
	aofWriteDB.Close() // wait for aof finished

	// start new db and read aof file
	aofReadDB := MustNewStandaloneServer()
	for i := 0; i < dbNum; i++ {
		conn.SelectDB(i)

		for j := 0; j < keySize1; j++ {
			key := strconv.Itoa(j)
			ret := aofReadDB.Exec(conn, utils.ToCmdLine("GET", key))
			asserts.AssertBulkReply(t, ret, key)
		}
		for j := 0; j < keySize2; j++ {
			key := "a" + strconv.Itoa(j)
			ret := aofReadDB.Exec(conn, utils.ToCmdLine("GET", key))
			asserts.AssertBulkReply(t, ret, key)
		}
	}
	aofReadDB.Close()
}
