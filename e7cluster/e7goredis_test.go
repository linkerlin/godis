// Real third-party client certification: the same two-node cluster the
// mini smart client is tested against, driven by go-redis v9's
// ClusterClient — the library most Go services actually run in production.
// It keeps its own slot map (CLUSTER SLOTS), follows -MOVED with a map
// reload and -ASK with one-shot ASKING retries, so these cases certify the
// full redirect/retry seam against a real client implementation.
//
//	go test ./e7cluster -run TestE7GoRedis -v
package e7cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/linkerlin/godis/lib/hashslot"
)

func mustGoRedis(t *testing.T, seed, joiner *node) *goredis.ClusterClient {
	t.Helper()
	rdb := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:        []string{seed.addr, joiner.addr},
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     8,
		MaxRetries:   2,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// mustGoRedisPinned returns a plain single-node client bound to addr.
func mustGoRedisPinned(t *testing.T, addr string) *goredis.Client {
	t.Helper()
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        addr,
		DialTimeout: 3 * time.Second,
		PoolSize:    4,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// TestE7GoRedisClusterInterop certifies: topology discovery, 200-key
// routing across both halves, hash-tag MSET/MGET, and — the core — a live
// reshard (markers → half migrated → client writes through the window →
// finish) survived entirely through go-redis's own redirect retries.
func TestE7GoRedisClusterInterop(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis interop skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	ranges := waitSlotsQuiet(t, seed, joiner)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	rdb := mustGoRedis(t, seed, joiner)

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("go-redis Ping: %v", err)
	}

	// 200 keys spread over both halves; go-redis routes each by its own
	// slot map (any stale view self-heals via -MOVED, which is part of
	// what this asserts)
	const total = 200
	for i := 0; i < total; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("gr:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("go-redis SET gr:%d: %v", i, err)
		}
	}
	for i := 0; i < total; i++ {
		v, err := rdb.Get(ctx, fmt.Sprintf("gr:%d", i)).Result()
		if err != nil || v != strconv.Itoa(i) {
			t.Fatalf("go-redis GET gr:%d = %q, %v", i, v, err)
		}
	}
	if err := rdb.MSet(ctx, map[string]interface{}{
		"{grtag}a": 1, "{grtag}b": 2, "{grtag}c": 3,
	}).Err(); err != nil {
		t.Fatalf("go-redis MSET hash-tag: %v", err)
	}
	vals, err := rdb.MGet(ctx, "{grtag}a", "{grtag}b", "{grtag}c").Result()
	if err != nil || len(vals) != 3 {
		t.Fatalf("go-redis MGET hash-tag: %v %v", vals, err)
	}

	// live reshard of one populated hash-tag slot, real client in the loop
	const n = 20
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("{grrs}%d", i)
	}
	slot := int(hashslot.Slot(keys[0]))
	exporter, _ := ownerOf(ranges, slot)
	importer := joiner.addr
	if exporter == joiner.addr {
		importer = seed.addr
	}
	t.Logf("slot %d live reshard %s -> %s with go-redis traffic", slot, exporter, importer)
	expRaw, impRaw := (&node{addr: exporter}).client(t), (&node{addr: importer}).client(t)
	defer expRaw.close()
	defer impRaw.close()
	for i, k := range keys {
		if r := expRaw.send("SET", k, "init"+strconv.Itoa(i)); r != "+OK" {
			t.Fatalf("seed SET %s: %s", k, r)
		}
	}
	if r := expRaw.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", importer); r != "+OK" {
		t.Fatalf("SETSLOT MIGRATING: %s", r)
	}
	if r := impRaw.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "IMPORTING", exporter); r != "+OK" {
		t.Fatalf("SETSLOT IMPORTING: %s", r)
	}
	host, port, _ := net.SplitHostPort(importer)
	migrate := func(ks []string) {
		t.Helper()
		args := append([]string{"MIGRATE", host, port, "", "0", "5000", "KEYS"}, ks...)
		if r := expRaw.send(args...); r != "+OK" {
			t.Fatalf("MIGRATE %d keys: %s", len(ks), r)
		}
	}
	migrate(keys[:n/2])

	// half-open window: go-redis writes+reads every key. Migrated keys come
	// back -ASK (its slot map still points at the exporter) and must be
	// retried with ASKING at the importer — transparently to this code
	for i, k := range keys {
		w := "win" + strconv.Itoa(i)
		if err := rdb.Set(ctx, k, w, 0).Err(); err != nil {
			t.Fatalf("go-redis SET %s mid-window: %v", k, err)
		}
		if v, err := rdb.Get(ctx, k).Result(); err != nil || v != w {
			t.Fatalf("go-redis GET %s mid-window = %q, %v", k, v, err)
		}
	}

	// finish the reshard, wait for the exporter to flip to -MOVED (FSM
	// apply lag), then read everything back through go-redis — first touch
	// of the flipped slot must self-heal via -MOVED + slot map reload
	migrate(keys[n/2:])
	if r := expRaw.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", importer); r != "+OK" {
		t.Fatalf("SETSLOT NODE (exporter): %s", r)
	}
	if r := impRaw.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", importer); r != "+OK" {
		t.Fatalf("SETSLOT NODE (importer): %s", r)
	}
	wantMoved := fmt.Sprintf("-MOVED %d %s", slot, importer)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if r := expRaw.send("GET", keys[0]); r == wantMoved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("exporter never flipped to MOVED\nseed logs:\n%s", seed.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	for i, k := range keys {
		if v, err := rdb.Get(ctx, k).Result(); err != nil || v != "win"+strconv.Itoa(i) {
			t.Fatalf("go-redis GET %s after reshard = %q, %v", k, v, err)
		}
	}
	// the pre-reshard keys must still answer too
	for i := 0; i < total; i++ {
		if v, err := rdb.Get(ctx, fmt.Sprintf("gr:%d", i)).Result(); err != nil || v != strconv.Itoa(i) {
			t.Fatalf("go-redis GET gr:%d after reshard = %q, %v", i, v, err)
		}
	}
}

// TestE7GoRedisShardedPubsub certifies sharded pub/sub (SSUBSCRIBE/SPUBLISH)
// through go-redis: both calls must route to the channel's slot owner and
// deliver the message.
func TestE7GoRedisShardedPubsub(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis sharded pubsub skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	waitSlotsQuiet(t, seed, joiner)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rdb := mustGoRedis(t, seed, joiner)

	const ch = "grsps:chan"
	sub := rdb.SSubscribe(ctx, ch)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil { // SSUBSCRIBE ack
		t.Fatalf("go-redis SSubscribe: %v", err)
	}
	if err := rdb.SPublish(ctx, ch, "hello").Err(); err != nil {
		t.Fatalf("go-redis SPublish: %v", err)
	}
	msg, err := sub.ReceiveTimeout(ctx, 3*time.Second)
	if err != nil {
		t.Fatalf("no sharded message within 3s: %v\nseed logs:\n%s\njoiner logs:\n%s",
			err, seed.out.String(), joiner.out.String())
	}
	if m, ok := msg.(*goredis.Message); !ok || m.Channel != ch || m.Payload != "hello" {
		t.Fatalf("sharded message = %#v, want channel %q payload %q", msg, ch, "hello")
	}
}

// TestE7GoRedisResp3Cluster certifies the cluster routing path over RESP3
// (HELLO 3 handshake on every pooled conn, map/array/double replies).
func TestE7GoRedisResp3Cluster(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis RESP3 cluster skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	waitSlotsQuiet(t, seed, joiner)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rdb := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:        []string{seed.addr, joiner.addr},
		Protocol:     3,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		PoolSize:     8,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("go-redis Ping RESP3: %v", err)
	}
	for i := 0; i < 60; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("gr3:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("RESP3 SET gr3:%d: %v", i, err)
		}
	}
	for i := 0; i < 60; i++ {
		if v, err := rdb.Get(ctx, fmt.Sprintf("gr3:%d", i)).Result(); err != nil || v != strconv.Itoa(i) {
			t.Fatalf("RESP3 GET gr3:%d = %q, %v", i, v, err)
		}
	}
	// hash in one slot: HGETALL is a RESP3 map reply through cluster routing
	if err := rdb.HSet(ctx, "{gr3tag}h", map[string]interface{}{"a": "1", "b": "2"}).Err(); err != nil {
		t.Fatalf("RESP3 HSET: %v", err)
	}
	m, err := rdb.HGetAll(ctx, "{gr3tag}h").Result()
	if err != nil || len(m) != 2 || m["a"] != "1" || m["b"] != "2" {
		t.Fatalf("RESP3 HGETALL = %v, %v", m, err)
	}
	vals, err := rdb.MGet(ctx, "gr3:0", "gr3:1", "{gr3tag}h").Result()
	if err != nil || len(vals) != 3 {
		t.Fatalf("RESP3 MGET = %v, %v", vals, err)
	}
}

// TestE7GoRedisClassicPubsubCrossNode pins the cluster PUBLISH broadcast:
// a subscriber pinned to node A must receive a message published to node B
// (Redis broadcasts classic pub/sub cluster-wide; only SPUBLISH is
// slot-routed). Both directions are asserted.
func TestE7GoRedisClassicPubsubCrossNode(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis classic pubsub cluster skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	waitSlotsQuiet(t, seed, joiner)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const ch = "grc:chan"
	cross := func(t *testing.T, subAddr, pubAddr, tag string) {
		t.Helper()
		sub, pub := mustGoRedisPinned(t, subAddr), mustGoRedisPinned(t, pubAddr)
		ps := sub.Subscribe(ctx, ch)
		defer ps.Close()
		if _, err := ps.Receive(ctx); err != nil {
			t.Fatalf("SUBSCRIBE on %s: %v", subAddr, err)
		}
		if err := pub.Publish(ctx, ch, tag).Err(); err != nil {
			t.Fatalf("PUBLISH via %s: %v", pubAddr, err)
		}
		msg, err := ps.ReceiveTimeout(ctx, 3*time.Second)
		if err != nil {
			t.Fatalf("no cross-node message %q within 3s (sub=%s pub=%s): %v\nseed logs:\n%s\njoiner logs:\n%s",
				tag, subAddr, pubAddr, err, seed.out.String(), joiner.out.String())
		}
		if m, ok := msg.(*goredis.Message); !ok || m.Channel != ch || m.Payload != tag {
			t.Fatalf("cross-node message = %#v, want %q", msg, tag)
		}
	}
	cross(t, seed.addr, joiner.addr, "seedSub_joinerPub")
	cross(t, joiner.addr, seed.addr, "joinerSub_seedPub")
}
