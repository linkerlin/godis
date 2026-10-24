package e6chaos

// Real-client persistence certification: e6chaos's zero-loss drills assert
// DBSIZE counts through the mini client; these tests verify the typed data
// plane across a restart with go-redis — every family's content survives a
// kill -9, TTL semantics persist (absolute timestamps), deletions and
// expirations stay applied, and the RDB load path round-trips too.
//
//	go test ./e6chaos -run TestE6GoRedis -v

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// mustChildGoRedis dials a child server with a real go-redis client.
func mustChildGoRedis(t *testing.T, cs *childServer) *goredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         cs.addr,
		Protocol:     2,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxRetries:   -1, // restart windows surface as errors, not silent retries
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestE6GoRedisAofRestart crashes a node mid-flight and verifies the full
// typed data plane after AOF replay: strings, counters, hashes, zsets, list
// element deletion, set element deletion, JSON documents, hash-field TTL,
// key TTL, DEL and expiry tombstones.
func TestE6GoRedisAofRestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	// always-fsync makes "acked == durable": the kill loses nothing acked,
	// so every assertion below is exact rather than lossy
	cs := startServer(t, dir, "appendonly yes", "appendfsync always")
	rdb := mustChildGoRedis(t, cs)
	ctx := context.Background()

	if err := rdb.Set(ctx, "ga:str", "v1", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	for i := 0; i < 10; i++ {
		if v, err := rdb.Incr(ctx, "ga:ctr").Result(); err != nil || v != int64(i+1) {
			t.Fatalf("INCR = %d, %v", v, err)
		}
	}
	if err := rdb.HSet(ctx, "ga:h", map[string]interface{}{"a": "1", "b": "2"}).Err(); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	if err := rdb.ZAdd(ctx, "ga:z", goredis.Z{Score: 1.5, Member: "m1"}, goredis.Z{Score: 2.5, Member: "m2"}).Err(); err != nil {
		t.Fatalf("ZADD: %v", err)
	}
	if err := rdb.RPush(ctx, "ga:l", "a", "b", "c").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if v, err := rdb.LPop(ctx, "ga:l").Result(); err != nil || v != "a" {
		t.Fatalf("LPOP = %q, %v", v, err)
	}
	if err := rdb.SAdd(ctx, "ga:s", "x", "y").Err(); err != nil {
		t.Fatalf("SADD: %v", err)
	}
	if n, err := rdb.SRem(ctx, "ga:s", "x").Result(); err != nil || n != 1 {
		t.Fatalf("SREM = %d, %v", n, err)
	}
	if err := rdb.Do(ctx, "JSON.SET", "ga:doc", "$", `{"a":1,"b":"two"}`).Err(); err != nil {
		t.Fatalf("JSON.SET: %v", err)
	}
	if err := rdb.HSet(ctx, "ga:hh", "f", "fv").Err(); err != nil {
		t.Fatalf("HSET ga:hh: %v", err)
	}
	// HEXPIRE replies with one status per field
	hex, err := rdb.Do(ctx, "HEXPIRE", "ga:hh", "120", "FIELDS", "1", "f").Result()
	if err != nil {
		t.Fatalf("HEXPIRE: %v", err)
	}
	arr, ok := hex.([]interface{})
	if !ok || len(arr) != 1 || arr[0] != int64(1) {
		t.Fatalf("HEXPIRE reply = %#v, want [1]", hex)
	}
	if err := rdb.Set(ctx, "ga:ttl", "v", 120*time.Second).Err(); err != nil {
		t.Fatalf("SET ex: %v", err)
	}
	// tombstones: a DELeted key and an already-expired key must stay gone
	if err := rdb.Set(ctx, "ga:gone", "v", 0).Err(); err != nil {
		t.Fatalf("SET gone: %v", err)
	}
	if n, err := rdb.Del(ctx, "ga:gone").Result(); err != nil || n != 1 {
		t.Fatalf("DEL = %d, %v", n, err)
	}
	if err := rdb.Set(ctx, "ga:expired", "v", 700*time.Millisecond).Err(); err != nil {
		t.Fatalf("SET expired: %v", err)
	}
	time.Sleep(900 * time.Millisecond)

	// crash: SIGKILL / TerminateProcess — nothing acked may be lost
	cs.kill()
	t.Cleanup(func() {}) // original cs already stopped; drop its cleanup work

	cs2 := startServer(t, dir, "appendonly yes", "appendfsync always")
	rdb2 := mustChildGoRedis(t, cs2)

	// poll the first read: AOF load finishes before the listener opens, but
	// give slow CI a moment
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, err := rdb2.Get(ctx, "ga:str").Result(); err == nil && v == "v1" {
			break
		} else if time.Now().After(deadline) {
			cs2.dieWithLogs(t, "ga:str = "+v+", AOF replay did not restore")
		}
		time.Sleep(100 * time.Millisecond)
	}

	if v, err := rdb2.Get(ctx, "ga:ctr").Result(); err != nil || v != "10" {
		t.Fatalf("counter after restart = %q, %v", v, err)
	}
	m, err := rdb2.HGetAll(ctx, "ga:h").Result()
	if err != nil || len(m) != 2 || m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("HGETALL after restart = %v, %v", m, err)
	}
	if s, err := rdb2.ZScore(ctx, "ga:z", "m2").Result(); err != nil || s != 2.5 {
		t.Fatalf("ZSCORE m2 after restart = %v, %v", s, err)
	}
	if n, err := rdb2.ZCard(ctx, "ga:z").Result(); err != nil || n != 2 {
		t.Fatalf("ZCARD after restart = %d, %v", n, err)
	}
	lst, err := rdb2.LRange(ctx, "ga:l", 0, -1).Result()
	if err != nil || len(lst) != 2 || lst[0] != "b" || lst[1] != "c" {
		t.Fatalf("LRANGE after restart = %v, %v — LPOP tombstone lost", lst, err)
	}
	if n, err := rdb2.SCard(ctx, "ga:s").Result(); err != nil || n != 1 {
		t.Fatalf("SCARD after restart = %d, %v — SREM tombstone lost", n, err)
	}
	doc, err := rdb2.Do(ctx, "JSON.GET", "ga:doc", "$").Result()
	if err != nil {
		t.Fatalf("JSON.GET after restart: %v", err)
	}
	// JSONPath $ returns the array of matched values
	if s, ok := doc.(string); !ok || len(s) == 0 || s[0] != '[' {
		t.Fatalf("JSON.GET after restart = %#v", doc)
	}
	// HTTL also replies with one value per field
	httlReply, err := rdb2.Do(ctx, "HTTL", "ga:hh", "FIELDS", "1", "f").Result()
	if err != nil {
		t.Fatalf("HTTL after restart: %v", err)
	}
	httlArr, ok := httlReply.([]interface{})
	if !ok || len(httlArr) != 1 {
		t.Fatalf("HTTL after restart = %#v", httlReply)
	}
	httl, _ := httlArr[0].(int64)
	if httl <= 0 || httl > 120 {
		t.Fatalf("HTTL after restart = %d — want (0,120]", httl)
	}
	ttl, err := rdb2.TTL(ctx, "ga:ttl").Result()
	if err != nil || ttl <= 0 || ttl > 120*time.Second {
		t.Fatalf("TTL after restart = %v, %v — want (0,120s]", ttl, err)
	}
	if n, err := rdb2.Exists(ctx, "ga:gone").Result(); err != nil || n != 0 {
		t.Fatalf("DELed key resurrected after restart: exists=%d, %v", n, err)
	}
	if n, err := rdb2.Exists(ctx, "ga:expired").Result(); err != nil || n != 0 {
		t.Fatalf("expired key resurrected after restart: exists=%d, %v", n, err)
	}
}

// TestE6GoRedisRdbRestart covers the RDB load path with a real client: SAVE
// snapshots, the process is killed without any AOF, and the restarted node
// must serve the snapshot content.
func TestE6GoRedisRdbRestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	cs := startServer(t, dir) // appendonly no, no auto-save
	rdb := mustChildGoRedis(t, cs)
	ctx := context.Background()

	if err := rdb.Set(ctx, "gr:str", "rv", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := rdb.HSet(ctx, "gr:h", map[string]interface{}{"k": "v"}).Err(); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	if err := rdb.ZAdd(ctx, "gr:z", goredis.Z{Score: 9.25, Member: "zm"}).Err(); err != nil {
		t.Fatalf("ZADD: %v", err)
	}
	if err := rdb.RPush(ctx, "gr:l", "x", "y").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	if err := rdb.Do(ctx, "SAVE").Err(); err != nil {
		t.Fatalf("SAVE: %v", err)
	}
	// written after the snapshot: only the snapshot persists, so this key
	// must NOT survive the restart (snapshot boundary)
	if err := rdb.Set(ctx, "gr:after", "late", 0).Err(); err != nil {
		t.Fatalf("SET after snapshot: %v", err)
	}
	cs.kill()

	cs2 := startServer(t, dir) // same dir: loads dump.rdb
	rdb2 := mustChildGoRedis(t, cs2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if v, err := rdb2.Get(ctx, "gr:str").Result(); err == nil && v == "rv" {
			break
		} else if time.Now().After(deadline) {
			cs2.dieWithLogs(t, "gr:str = "+v+", RDB load did not restore")
		}
		time.Sleep(100 * time.Millisecond)
	}

	m, err := rdb2.HGetAll(ctx, "gr:h").Result()
	if err != nil || len(m) != 1 || m["k"] != "v" {
		t.Fatalf("HGETALL after restart = %v, %v", m, err)
	}
	if s, err := rdb2.ZScore(ctx, "gr:z", "zm").Result(); err != nil || s != 9.25 {
		t.Fatalf("ZSCORE after restart = %v, %v", s, err)
	}
	lst, err := rdb2.LRange(ctx, "gr:l", 0, -1).Result()
	if err != nil || len(lst) != 2 || lst[0] != "x" || lst[1] != "y" {
		t.Fatalf("LRANGE after restart = %v, %v", lst, err)
	}
	// snapshot boundary: written after SAVE, absent from the RDB
	if n, err := rdb2.Exists(ctx, "gr:after").Result(); err != nil || n != 0 {
		t.Fatalf("post-snapshot write survived restart: exists=%d, %v", n, err)
	}
}
