package gnet

// Real-client certification for the gnet frontend: the async event loop has a
// different reply path than the std frontend (batched outBuf writes per
// OnTraffic, blocking commands dispatched to a goroutine replying via
// AsyncWrite, pub/sub pushes written from the hub goroutine, half-packet
// retention across OnTraffic calls) — the same go-redis v9 battery that
// certifies std must pass over this transport too.
//
//	go test ./redis/server/gnet -run TestE2EGoRedisGnet -v

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/linkerlin/godis/database"
)

func startGoRedisGnetServer(t *testing.T) string {
	t.Helper()
	// grab a free port, then hand it to gnet (gnet.Run has no :0 discovery)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	handler := NewGnetServer(database.MustNewStandaloneServer())
	go func() { _ = handler.Run(addr) }()
	t.Cleanup(func() { _ = handler.Close() })

	for i := 0; i < 200; i++ {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("gnet server did not become ready")
	return ""
}

func mustGoRedisGnet(t *testing.T, addr string, protocol int) *goredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{
		Addr:         addr,
		Protocol:     protocol,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// e2eGoRedisGnetCore drives the std frontend's core battery over the async
// frontend; map/double reply shapes differ between RESP2 and RESP3, so both
// modes run it.
func e2eGoRedisGnetCore(t *testing.T, rdb *goredis.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := rdb.Set(ctx, "gr:str", "v1", 0).Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if v, err := rdb.Get(ctx, "gr:str").Result(); err != nil || v != "v1" {
		t.Fatalf("GET = %q, %v", v, err)
	}
	if v, err := rdb.Incr(ctx, "gr:ctr").Result(); err != nil || v != 1 {
		t.Fatalf("INCR = %d, %v", v, err)
	}
	if v, err := rdb.IncrBy(ctx, "gr:ctr", 41).Result(); err != nil || v != 42 {
		t.Fatalf("INCRBY = %d, %v", v, err)
	}

	// hash: HGETALL is a map reply in RESP3, a flat array in RESP2
	if err := rdb.HSet(ctx, "gr:hash", map[string]interface{}{"a": "1", "b": "2"}).Err(); err != nil {
		t.Fatalf("HSET: %v", err)
	}
	m, err := rdb.HGetAll(ctx, "gr:hash").Result()
	if err != nil || len(m) != 2 || m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("HGETALL = %v, %v", m, err)
	}

	// zset: ZSCORE is a double in RESP3, bulk in RESP2
	if err := rdb.ZAdd(ctx, "gr:z", goredis.Z{Score: 3.5, Member: "m"}).Err(); err != nil {
		t.Fatalf("ZADD: %v", err)
	}
	if s, err := rdb.ZScore(ctx, "gr:z", "m").Result(); err != nil || s != 3.5 {
		t.Fatalf("ZSCORE = %v, %v", s, err)
	}

	// list + blocking consumer with a late producer on a second client —
	// on gnet this exercises the blocking-command goroutine + AsyncWrite path
	if err := rdb.RPush(ctx, "gr:list", "x").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	prod := mustGoRedisGnet(t, rdb.Options().Addr, rdb.Options().Protocol)
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = prod.RPush(context.Background(), "gr:block", "wake").Err()
	}()
	bl, err := rdb.BLPop(ctx, 3*time.Second, "gr:block").Result()
	if err != nil {
		t.Fatalf("BLPOP: %v", err)
	}
	if len(bl) != 2 || bl[1] != "wake" {
		t.Fatalf("BLPOP = %v", bl)
	}

	// ttl path
	if err := rdb.Set(ctx, "gr:ttl", "v", 10*time.Second).Err(); err != nil {
		t.Fatalf("SET ex: %v", err)
	}
	if d, err := rdb.TTL(ctx, "gr:ttl").Result(); err != nil || d <= 0 || d > 10*time.Second {
		t.Fatalf("TTL = %v, %v", d, err)
	}

	// pipelining: 100 SET + 100 INCR in one round trip (outBuf batched write)
	pipe := rdb.Pipeline()
	for i := 0; i < 100; i++ {
		pipe.Set(ctx, fmt.Sprintf("gr:p:%d", i), i, 0)
		pipe.Incr(ctx, "gr:pctr")
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}
	if v, err := rdb.Get(ctx, "gr:p:57").Result(); err != nil || v != "57" {
		t.Fatalf("pipeline GET = %q, %v", v, err)
	}
	if v, err := rdb.Get(ctx, "gr:pctr").Result(); err != nil || v != "100" {
		t.Fatalf("pipeline INCR total = %q, %v", v, err)
	}

	// scripting: EVAL with keys/argv, then SCRIPT LOAD + EVALSHA
	if v, err := rdb.Eval(ctx, "return redis.call('SET', KEYS[1], ARGV[1])", []string{"gr:lua"}, "lv").Result(); err != nil || fmt.Sprint(v) != "OK" {
		t.Fatalf("EVAL = %v, %v", v, err)
	}
	sha, err := rdb.ScriptLoad(ctx, "return {ARGV[1], ARGV[2]}").Result()
	if err != nil {
		t.Fatalf("SCRIPT LOAD: %v", err)
	}
	rv, err := rdb.EvalSha(ctx, sha, []string{}, "a", "b").Result()
	if err != nil {
		t.Fatalf("EVALSHA: %v", err)
	}
	arr, ok := rv.([]interface{})
	if !ok || len(arr) != 2 || arr[0] != "a" || arr[1] != "b" {
		t.Fatalf("EVALSHA reply = %#v", rv)
	}
}

func TestE2EGoRedisGnetRESP2(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis gnet e2e skipped in short mode")
	}
	rdb := mustGoRedisGnet(t, startGoRedisGnetServer(t), 2)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping RESP2: %v", err)
	}
	e2eGoRedisGnetCore(t, rdb)
}

func TestE2EGoRedisGnetRESP3(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis gnet e2e skipped in short mode")
	}
	rdb := mustGoRedisGnet(t, startGoRedisGnetServer(t), 3)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping RESP3 (HELLO 3): %v", err)
	}
	e2eGoRedisGnetCore(t, rdb)
}

func TestE2EGoRedisGnetPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis gnet pubsub e2e skipped in short mode")
	}
	addr := startGoRedisGnetServer(t)
	sub := mustGoRedisGnet(t, addr, 2)
	pub := mustGoRedisGnet(t, addr, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ps := sub.Subscribe(ctx, "gr:chan")
	defer ps.Close()
	if _, err := ps.Receive(ctx); err != nil { // SUBSCRIBE ack
		t.Fatalf("SUBSCRIBE: %v", err)
	}
	if err := pub.Publish(ctx, "gr:chan", "hello").Err(); err != nil {
		t.Fatalf("PUBLISH: %v", err)
	}
	msg, err := ps.ReceiveTimeout(ctx, 3*time.Second)
	if err != nil {
		t.Fatalf("no message within 3s: %v", err)
	}
	if m, ok := msg.(*goredis.Message); !ok || m.Channel != "gr:chan" || m.Payload != "hello" {
		t.Fatalf("message = %#v", msg)
	}

	// pattern subscription
	pps := sub.PSubscribe(ctx, "gr:pat:*")
	defer pps.Close()
	if _, err := pps.Receive(ctx); err != nil {
		t.Fatalf("PSUBSCRIBE: %v", err)
	}
	if err := pub.Publish(ctx, "gr:pat:x", "phello").Err(); err != nil {
		t.Fatalf("PUBLISH pattern: %v", err)
	}
	pmsg, err := pps.ReceiveTimeout(ctx, 3*time.Second)
	if err != nil {
		t.Fatalf("no pattern message within 3s: %v", err)
	}
	if m, ok := pmsg.(*goredis.Message); !ok || m.Channel != "gr:pat:x" || m.Payload != "phello" {
		t.Fatalf("pattern message = %#v", pmsg)
	}
}

// TestE2EGoRedisGnetBinarySafety pins framing safety on the async frontend: a
// 1MB blob studded with RESP metacharacters must round-trip byte-identical —
// the frame spans multiple OnTraffic calls, exercising half-packet retention,
// and a unicode key rides along.
func TestE2EGoRedisGnetBinarySafety(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis gnet binary safety skipped in short mode")
	}
	rdb := mustGoRedisGnet(t, startGoRedisGnetServer(t), 2)
	ctx := context.Background()

	rng := rand.New(rand.NewSource(42))
	blob := make([]byte, 1<<20)
	rng.Read(blob)
	for _, off := range []int{0, 4096, 333333, 1<<20 - 16} {
		copy(blob[off:], "*1\r\n$3\r\nGET\r\n+-:,\r\n\x00\xff")
	}
	if err := rdb.Set(ctx, "gr:bin", string(blob), 0).Err(); err != nil {
		t.Fatalf("SET binary: %v", err)
	}
	got, err := rdb.Get(ctx, "gr:bin").Bytes()
	if err != nil {
		t.Fatalf("GET binary: %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Fatalf("binary roundtrip mismatch: got %d bytes, want %d", len(got), len(blob))
	}
	const ukey = "gr:键:क-vs-🌍"
	if err := rdb.Set(ctx, ukey, "uv", 0).Err(); err != nil {
		t.Fatalf("SET unicode key: %v", err)
	}
	if v, err := rdb.Get(ctx, ukey).Result(); err != nil || v != "uv" {
		t.Fatalf("GET unicode key = %q, %v", v, err)
	}
}

// TestE2EGoRedisGnetTransactions certifies MULTI/EXEC and optimistic WATCH
// with a real client: TxPipeline queuing and reply-position commands, plus a
// conflicting write from another client between WATCH and EXEC aborting the
// first attempt — go-redis re-runs the function on ErrWatch, so the
// interference must be detected exactly once and the second attempt commits.
func TestE2EGoRedisGnetTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis gnet transactions e2e skipped in short mode")
	}
	addr := startGoRedisGnetServer(t)
	rdb := mustGoRedisGnet(t, addr, 2)
	other := mustGoRedisGnet(t, addr, 2)
	ctx := context.Background()

	// TxPipeline: MULTI ... EXEC with reply-position commands
	pipe := rdb.TxPipeline()
	setCmd := pipe.Set(ctx, "grtx:a", "1", 0)
	incrCmd := pipe.Incr(ctx, "grtx:ctr")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("TxPipeline exec: %v", err)
	}
	if err := setCmd.Err(); err != nil || setCmd.Val() != "OK" {
		t.Fatalf("tx SET = %v, %v", setCmd.Val(), err)
	}
	if v, err := incrCmd.Result(); err != nil || v != 1 {
		t.Fatalf("tx INCR = %d, %v", v, err)
	}

	// optimistic locking across a conflicting write. go-redis surfaces a
	// watch abort as TxFailedErr and does NOT retry internally — the caller
	// owns the optimistic-retry loop (official pattern).
	attempts := 0
	for {
		err := rdb.Watch(ctx, func(tx *goredis.Tx) error {
			attempts++
			v, err := tx.Get(ctx, "grtx:ctr").Int()
			if err != nil && err != goredis.Nil {
				return err
			}
			if attempts == 1 {
				if err := other.Set(ctx, "grtx:ctr", 999, 0).Err(); err != nil {
					return err
				}
			}
			_, err = tx.TxPipelined(ctx, func(p goredis.Pipeliner) error {
				p.Set(ctx, "grtx:res", v+1, 0)
				return nil
			})
			return err
		}, "grtx:ctr")
		if err == goredis.TxFailedErr {
			continue // watch aborted — optimistic retry
		}
		if err != nil {
			t.Fatalf("Watch: %v", err)
		}
		break
	}
	if attempts != 2 {
		t.Fatalf("Watch attempts = %d, want 2 (conflict must abort attempt 1)", attempts)
	}
	// attempt 1 read ctr=1 (res=2) and was aborted; attempt 2 read 999 and
	// committed res=1000 — the committed value proves EXEC was retried
	if v, err := rdb.Get(ctx, "grtx:res").Result(); err != nil || v != "1000" {
		t.Fatalf("grtx:res = %q, %v — want attempt-2 commit 1000", v, err)
	}
	// the interfering write must have survived (that's what aborted attempt 1)
	if v, err := rdb.Get(ctx, "grtx:ctr").Result(); err != nil || v != "999" {
		t.Fatalf("grtx:ctr = %q, %v — want interfering write 999 to survive", v, err)
	}
}
