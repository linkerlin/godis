package std

// Real-client standalone certification: the same in-process std server the
// e2e benchmark drives, exercised through go-redis v9 — the paths every
// production Go service uses. Covers both RESP2 and RESP3 (HELLO 3) wire
// modes, pipelining, blocking lists, classic pub/sub, and Lua scripting.
//
//	go test ./redis/server/std -run TestE2EGoRedis -v

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/linkerlin/godis/tcp"
)

func startGoRedisServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := MakeHandler()
	if err != nil {
		t.Fatal(err)
	}
	closeChan := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() {
		close(closeChan)
		_ = handler.Close()
		<-done
	})
	go func() {
		defer close(done)
		tcp.ListenAndServe(listener, handler, closeChan)
	}()
	return listener.Addr().String()
}

func mustGoRedis(t *testing.T, addr string, protocol int) *goredis.Client {
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

// e2eGoRedisCore drives the common data-path commands over the given
// protocol version; replies like HGETALL maps and ZSCORE doubles take
// different wire shapes in RESP2 vs RESP3, so parsing them in both modes
// is the point.
func e2eGoRedisCore(t *testing.T, rdb *goredis.Client) {
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

	// list + blocking consumer with a late producer on a second client
	if err := rdb.RPush(ctx, "gr:list", "x").Err(); err != nil {
		t.Fatalf("RPUSH: %v", err)
	}
	prod := mustGoRedis(t, rdb.Options().Addr, rdb.Options().Protocol)
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

	// pipelining: 100 SET + 100 INCR in one round trip
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

func TestE2EGoRedisStandaloneRESP2(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis standalone e2e skipped in short mode")
	}
	rdb := mustGoRedis(t, startGoRedisServer(t), 2)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping RESP2: %v", err)
	}
	e2eGoRedisCore(t, rdb)
}

func TestE2EGoRedisStandaloneRESP3(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis standalone e2e skipped in short mode")
	}
	rdb := mustGoRedis(t, startGoRedisServer(t), 3)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping RESP3 (HELLO 3): %v", err)
	}
	e2eGoRedisCore(t, rdb)
}

func TestE2EGoRedisStandalonePubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis pubsub e2e skipped in short mode")
	}
	addr := startGoRedisServer(t)
	sub := mustGoRedis(t, addr, 2)
	pub := mustGoRedis(t, addr, 2)
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
