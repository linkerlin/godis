package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/memory"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestE0_ConcurrentReadLazyExpireAndScanRace verifies that concurrent read commands
// (GET, HGET, SCAN, KEYS) racing with short TTL lazy/active expirations never cause
// concurrent map read/write crashes, re-entrant RLock->Lock deadlocks, or data races.
func TestE0_ConcurrentReadLazyExpireAndScanRace(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	const workers = 16
	const iterations = 60
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := connection.NewFakeConn()
			for i := 0; i < iterations; i++ {
				k := fmt.Sprintf("e0:key:%d", (id+i)%12)
				hk := fmt.Sprintf("e0:hash:%d", (id+i)%8)
				switch (id + i) % 5 {
				case 0:
					srv.Exec(conn, utils.ToCmdLine("SET", k, "val", "PX", "2"))
				case 1:
					srv.Exec(conn, utils.ToCmdLine("GET", k))
				case 2:
					srv.Exec(conn, utils.ToCmdLine("HSET", hk, "f1", "v1", "f2", "v2"))
					srv.Exec(conn, utils.ToCmdLine("HPEXPIRE", hk, "2", "FIELDS", "1", "f1"))
				case 3:
					srv.Exec(conn, utils.ToCmdLine("HGET", hk, "f1"))
					srv.Exec(conn, utils.ToCmdLine("HLEN", hk))
				case 4:
					srv.Exec(conn, utils.ToCmdLine("SCAN", "0", "COUNT", "5"))
					srv.Exec(conn, utils.ToCmdLine("KEYS", "e0:*"))
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestE0_WatchDetectsTTLExpiryAndDeleteABA verifies that WATCH properly aborts a transaction
// when a watched key expires before EXEC, or when a watched key is deleted and recreated.
func TestE0_WatchDetectsTTLExpiryAndDeleteABA(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	c1 := connection.NewFakeConn()
	c2 := connection.NewFakeConn()

	// Case 1: Key expires while watched
	srv.Exec(c1, utils.ToCmdLine("SET", "watch:ttl", "v1", "PX", "20"))
	srv.Exec(c1, utils.ToCmdLine("WATCH", "watch:ttl"))
	time.Sleep(35 * time.Millisecond)
	// Trigger lazy expire via a read
	srv.Exec(c2, utils.ToCmdLine("GET", "watch:ttl"))

	srv.Exec(c1, utils.ToCmdLine("MULTI"))
	srv.Exec(c1, utils.ToCmdLine("SET", "watch:ttl", "tx_val"))
	res := srv.Exec(c1, utils.ToCmdLine("EXEC"))
	if _, ok := res.(*protocol.NullMultiBulkReply); !ok {
		t.Fatalf("expected NullMultiBulkReply when watched key expired, got %T (%s)", res, res.ToBytes())
	}

	// Case 2: Delete + recreate ABA check
	srv.Exec(c1, utils.ToCmdLine("SET", "watch:aba", "v1"))
	srv.Exec(c1, utils.ToCmdLine("WATCH", "watch:aba"))
	srv.Exec(c2, utils.ToCmdLine("DEL", "watch:aba"))

	srv.Exec(c1, utils.ToCmdLine("MULTI"))
	srv.Exec(c1, utils.ToCmdLine("SET", "watch:aba", "tx_val"))
	res = srv.Exec(c1, utils.ToCmdLine("EXEC"))
	if _, ok := res.(*protocol.NullMultiBulkReply); !ok {
		t.Fatalf("expected NullMultiBulkReply when watched key was deleted, got %T (%s)", res, res.ToBytes())
	}
}

// TestE0_StreamConsumerGroupConcurrentRace verifies thread-safe ConsumerGroup access
// under concurrent XADD, XREADGROUP, XACK, XPENDING, and XINFO.
func TestE0_StreamConsumerGroupConcurrentRace(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	c := connection.NewFakeConn()
	srv.Exec(c, utils.ToCmdLine("XGROUP", "CREATE", "s:race", "g1", "0", "MKSTREAM"))
	for i := 0; i < 40; i++ {
		srv.Exec(c, utils.ToCmdLine("XADD", "s:race", "*", "f", strconv.Itoa(i)))
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := connection.NewFakeConn()
			consumer := fmt.Sprintf("c%d", id%3)
			for i := 0; i < 20; i++ {
				srv.Exec(conn, utils.ToCmdLine("XREADGROUP", "GROUP", "g1", consumer, "COUNT", "2", "STREAMS", "s:race", ">"))
				srv.Exec(conn, utils.ToCmdLine("XPENDING", "s:race", "g1", "-", "+", "5"))
				srv.Exec(conn, utils.ToCmdLine("XINFO", "GROUPS", "s:race"))
				srv.Exec(conn, utils.ToCmdLine("XINFO", "CONSUMERS", "s:race", "g1"))
			}
		}(w)
	}
	wg.Wait()
}

// TestE0_ClientCacheIdempotentEnableAndConcurrentInvalidate verifies that repeated
// EnableTracking calls do not leak goroutines or inflate tracking_clients count,
// and concurrent InvalidateKeyFrom is race-free.
func TestE0_ClientCacheIdempotentEnableAndConcurrentInvalidate(t *testing.T) {
	base := GetTrackingClientsCount()
	c1 := connection.NewFakeConn()
	var cid1 string
	for i := 0; i < 5; i++ {
		cid1 = EnableTracking(c1, "default", nil, "", false)
	}
	if got := GetTrackingClientsCount() - base; got != 1 {
		DisableTracking(cid1)
		t.Fatalf("expected +1 tracking_clients after idempotent EnableTracking, got +%d", got)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := connection.NewFakeConn()
			cid := EnableTracking(conn, "default", nil, "", false)
			for i := 0; i < 30; i++ {
				k := fmt.Sprintf("cache:key:%d", i%5)
				TrackKey(cid, k)
				InvalidateKeyFrom(k, "other")
			}
			DisableTracking(cid)
		}(w)
	}
	wg.Wait()
	DisableTracking(cid1)
	if got := GetTrackingClientsCount(); got != base {
		t.Fatalf("expected tracking_clients=%d after DisableTracking all, got %d", base, got)
	}
}

// TestE0_CrossDBExpireTaskIsolation verifies that setting TTL on the same key name
// in DB 0 and DB 1 uses distinct timewheel task keys and does not overwrite each other.
func TestE0_CrossDBExpireTaskIsolation(t *testing.T) {
	if config.Properties.Databases < 4 {
		config.Properties.Databases = 16
	}
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	db0, err := srv.selectDBSafe(0)
	if err != nil || db0 == nil {
		t.Fatalf("selectDBSafe(0) failed: %v", err)
	}
	db1, err := srv.selectDBSafe(1)
	if err != nil || db1 == nil {
		t.Fatalf("selectDBSafe(1) failed: %v", err)
	}
	if db0.genExpireTask("shared") == db1.genExpireTask("shared") {
		t.Fatalf("expected distinct expire task keys across DBs, got %q", db0.genExpireTask("shared"))
	}
}

// TestE1_AofRewritePreservesRedisFunctions verifies that Redis Functions loaded via
// FUNCTION LOAD survive AOF rewrite (both pure-AOF and RDB-preamble modes).
func TestE1_AofRewritePreservesRedisFunctions(t *testing.T) {
	for _, usePreamble := range []bool{false, true} {
		t.Run(fmt.Sprintf("preamble_%v", usePreamble), func(t *testing.T) {
			tmpDir := t.TempDir()
			aofFile := filepath.Join(tmpDir, "appendonly.aof")

			origAppendOnly := config.Properties.AppendOnly
			origAppendFile := config.Properties.AppendFilename
			origPreamble := config.Properties.AofUseRdbPreamble
			origDBs := config.Properties.Databases
			defer func() {
				config.Properties.AppendOnly = origAppendOnly
				config.Properties.AppendFilename = origAppendFile
				config.Properties.AofUseRdbPreamble = origPreamble
				config.Properties.Databases = origDBs
				_ = os.Remove(aofFile)
			}()

			config.Properties.AppendOnly = true
			config.Properties.AppendFilename = aofFile
			config.Properties.AofUseRdbPreamble = usePreamble
			config.Properties.Databases = 4

			srv1, err := NewStandaloneServer()
			if err != nil {
				t.Fatalf("NewStandaloneServer failed: %v", err)
			}
			conn := connection.NewFakeConn()
			srv1.Exec(conn, utils.ToCmdLine("FUNCTION", "FLUSH"))

			libCode := "#!lua name=evolib\nredis.register_function('evofn', function(keys, args) return redis.call('GET', keys[1]) end)"
			res := srv1.Exec(conn, utils.ToCmdLine("FUNCTION", "LOAD", "REPLACE", libCode))
			if protocol.IsErrorReply(res) {
				srv1.Close()
				t.Fatalf("FUNCTION LOAD failed: %s", res.ToBytes())
			}
			srv1.Exec(conn, utils.ToCmdLine("SET", "evo:k1", "hello_func"))

			if err := srv1.persister.Rewrite(); err != nil {
				srv1.Close()
				t.Fatalf("Rewrite failed: %v", err)
			}
			srv1.Close()

			// Clear in-memory functions to verify reload from rewritten AOF
			RestoreFunctionLoadCmds(nil)

			srv2, err := NewStandaloneServer()
			if err != nil {
				t.Fatalf("NewStandaloneServer reload failed: %v", err)
			}
			defer srv2.Close()

			out := srv2.Exec(conn, utils.ToCmdLine("FCALL", "evofn", "1", "evo:k1"))
			asserts.AssertBulkReply(t, out, "hello_func")
		})
	}
}

// TestE1_ConcurrentLuaEvalAndEvalROIsolation verifies that concurrent EVAL, EVAL_RO,
// and FCALL across multiple connections and DBs are race-free and preserve read-only enforcement.
func TestE1_ConcurrentLuaEvalAndEvalROIsolation(t *testing.T) {
	if config.Properties.Databases < 4 {
		config.Properties.Databases = 16
	}
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	c0 := connection.NewFakeConn()
	srv.Exec(c0, utils.ToCmdLine("SET", "lua:ro:key", "safe"))

	var wg sync.WaitGroup
	for w := 0; w < 10; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn := connection.NewFakeConn()
			conn.SelectDB(id % 2)
			for i := 0; i < 20; i++ {
				if id%2 == 0 {
					r := srv.Exec(conn, utils.ToCmdLine("EVAL_RO", "return redis.call('SET', 'lua:ro:key', 'bad')", "1", "lua:ro:key"))
					if !protocol.IsErrorReply(r) {
						t.Errorf("expected EVAL_RO write to fail, got %s", r.ToBytes())
					}
				} else {
					k := fmt.Sprintf("lua:rw:%d", id)
					v := fmt.Sprintf("val_%d", i)
					r := srv.Exec(conn, utils.ToCmdLine("EVAL", "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])", "1", k, v))
					asserts.AssertBulkReply(t, r, v)
				}
			}
		}(w)
	}
	wg.Wait()
}

// TestE1_NestedEvalFromLuaIsRejected verifies that redis.call('EVAL'...) is
// rejected instead of re-entering scriptExecMu on the same goroutine (deadlock).
func TestE1_NestedEvalFromLuaIsRejected(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	c := connection.NewFakeConn()
	r := srv.Exec(c, utils.ToCmdLine("EVAL", "return redis.call('EVAL', 'return 1', '0')", "0"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("nested EVAL should be rejected, got %s", r.ToBytes())
	}
	if !strings.Contains(string(r.ToBytes()), "not allowed from script") {
		t.Fatalf("unexpected nested EVAL error: %s", r.ToBytes())
	}

	// FCALL from a script must be rejected for the same reason.
	r = srv.Exec(c, utils.ToCmdLine("EVAL", "return redis.call('FCALL', 'nofn', '0')", "0"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "not allowed from script") {
		t.Fatalf("nested FCALL should be rejected, got %s", r.ToBytes())
	}
}

// TestE2_ShardedEvictionConcurrentTouchAndEvict verifies that sharded EvictionManager
// handles concurrent Touch, Modify, IdleSeconds, Freq, and EvictKeys without race conditions.
func TestE2_ShardedEvictionConcurrentTouchAndEvict(t *testing.T) {
	db := makeTestDB()
	em := NewEvictionManager(db, memory.AllKeysLFU)
	db.evictionManager = em

	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("ev:k:%d", i)
		db.PutEntity(k, nil)
		em.Touch(k)
		em.Modify(k)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				k := fmt.Sprintf("ev:k:%d", (id+i)%50)
				em.Touch(k)
				em.Modify(k)
				_ = em.IdleSeconds(k)
				_ = em.Freq(k)
			}
		}(w)
	}
	wg.Wait()
	_ = em.EvictKeys(256)
}

// TestE1_AofConcurrentSaveCmdLineAndClose verifies that concurrent SaveCmdLine
// calls racing with Persister.Close() never panic with send on closed channel.
func TestE1_AofConcurrentSaveCmdLineAndClose(t *testing.T) {
	tmpDir := t.TempDir()
	aofFile := filepath.Join(tmpDir, "concurrent_close.aof")

	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	p, err := NewPersister(srv, aofFile, false, "everysec")
	if err != nil {
		t.Fatalf("NewPersister failed: %v", err)
	}

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				p.SaveCmdLine(0, utils.ToCmdLine("SET", fmt.Sprintf("k:%d:%d", id, i), "v"))
			}
		}(w)
	}
	time.Sleep(2 * time.Millisecond)
	p.Close()
	p.Close() // idempotent close
	wg.Wait()
}

// TestE2_HImportWatchAndNullClientSafety verifies that HIMPORT SET goes through
// the normal HSET execution pipeline (bumping WATCH versions) and rejects nil clients safely.
func TestE2_HImportWatchAndNullClientSafety(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	// Nil client safety
	nilRes := srv.Exec(nil, utils.ToCmdLine("HIMPORT", "SET", "h:imp", "fs1", "v1"))
	if !protocol.IsErrorReply(nilRes) {
		t.Fatalf("expected error for HIMPORT SET with nil client, got %s", nilRes.ToBytes())
	}

	c1 := connection.NewFakeConn()
	c2 := connection.NewFakeConn()

	srv.Exec(c1, utils.ToCmdLine("HSET", "h:watch", "f1", "old"))
	srv.Exec(c1, utils.ToCmdLine("WATCH", "h:watch"))

	// Modify watched hash via HIMPORT SET on c2
	asserts.AssertStatusReply(t, srv.Exec(c2, utils.ToCmdLine("HIMPORT", "PREPARE", "fs1", "f1", "f2")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c2, utils.ToCmdLine("HIMPORT", "SET", "h:watch", "fs1", "new1", "new2")), "OK")

	srv.Exec(c1, utils.ToCmdLine("MULTI"))
	srv.Exec(c1, utils.ToCmdLine("HSET", "h:watch", "f1", "tx"))
	execRes := srv.Exec(c1, utils.ToCmdLine("EXEC"))
	if _, ok := execRes.(*protocol.NullMultiBulkReply); !ok {
		t.Fatalf("expected WATCH to abort EXEC after HIMPORT SET modified key, got %T (%s)", execRes, execRes.ToBytes())
	}
}

// TestE4_SlowlogArgcAndStringTruncation verifies Redis 8.10 SLOWLOG GET truncation
// (>32 args and >128 byte strings) while preserving the true 7th field Argc.
func TestE4_SlowlogArgcAndStringTruncation(t *testing.T) {
	sl := NewSlowLogger(16, 0)
	longVal := make([]byte, 150)
	for i := range longVal {
		longVal[i] = 'a'
	}
	args := make([][]byte, 36)
	args[0] = []byte("MSET")
	args[1] = []byte("k1")
	args[2] = longVal
	for i := 3; i < 36; i++ {
		args[i] = []byte(fmt.Sprintf("arg_%d", i))
	}
	sl.Record(time.Now().Add(-10*time.Millisecond), args, "127.0.0.1:12345", "test-client")

	entries := sl.GetEntries(1)
	if len(entries) != 1 {
		t.Fatalf("expected 1 slowlog entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Argc != 36 {
		t.Fatalf("expected true Argc=36, got %d", e.Argc)
	}
	if len(e.Command) != 32 {
		t.Fatalf("expected truncated Command len=32, got %d", len(e.Command))
	}
	if string(e.Command[31]) != "... (5 more arguments)" {
		t.Fatalf("unexpected trailing arg marker: %q", string(e.Command[31]))
	}
	wantTruncStr := string(longVal[:128]) + "... (22 more bytes)"
	if string(e.Command[2]) != wantTruncStr {
		t.Fatalf("unexpected truncated string: %q", string(e.Command[2]))
	}
}

// TestE4_JSONPathNestedProjectionsParenArithAndKeyOperator verifies Redis 8.10
// JSONPath nested function projections, parenthesized arithmetic in filters,
// prefix function calls in filters, and the `~` property-name operator.
func TestE4_JSONPathNestedProjectionsParenArithAndKeyOperator(t *testing.T) {
	srv, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer failed: %v", err)
	}
	defer srv.Close()

	c := connection.NewFakeConn()
	doc := `{"nums":[-10,-3,4],"obj":{"b":1,"a":2},"items":[{"a":2,"b":3,"x":6,"name":"cat"},{"a":1,"b":1,"x":20,"name":"elephant"}]}`
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("JSON.SET", "jp:doc", "$", doc)), "OK")

	// 1. Nested top-level projection: abs(min($.nums)) -> 10
	r1 := srv.Exec(c, utils.ToCmdLine("JSON.GET", "jp:doc", "abs(min($.nums))"))
	asserts.AssertBulkReply(t, r1, "10")

	// 2. Nested top-level projection: length(keys($.obj)) -> 2
	r2 := srv.Exec(c, utils.ToCmdLine("JSON.GET", "jp:doc", "length(keys($.obj))"))
	asserts.AssertBulkReply(t, r2, "2")

	// 3. Property-name operator `~`: $.obj.~ -> [["a","b"]]
	r3 := srv.Exec(c, utils.ToCmdLine("JSON.GET", "jp:doc", "$.obj.~"))
	asserts.AssertBulkReply(t, r3, `[["a","b"]]`)

	// 4. Filter with parenthesized arithmetic and prefix function calls:
	//    ($.items[?((@.a + @.b) * 2 == 10 && abs(@.x - 5) <= 2 && length(@.name) == 3)].name) -> ["cat"]
	r4 := srv.Exec(c, utils.ToCmdLine("JSON.GET", "jp:doc", "$.items[?((@.a + @.b) * 2 == 10 && abs(@.x - 5) <= 2 && length(@.name) == 3)].name"))
	asserts.AssertBulkReply(t, r4, `["cat"]`)
}
