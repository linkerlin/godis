// Cluster authentication + ACL certification with a real client: requirepass
// and ACL users are enforced on the cluster dispatch path (the same gate the
// standalone server uses), go-redis's ClusterClient authenticates every pooled
// connection across both nodes, and a restricted user set identically on each
// node is enforced per node. ACL state is node-local in real Redis too — the
// test applies the identical SETUSER to both nodes, which is the documented
// ops practice (aclfile shipped to every node).
//
//   - TestE7GoRedisClusterAuthAcl: requirepass hot-set, NOAUTH/WRONGPASS
//     gates, credentialed ClusterClient across both halves, ACL user across
//     both halves with NOPERM on denied commands and key patterns.
//
// Note: config-time requirepass would break the join handshake — the joiner
// sends `cluster.join` over a plain client connection (godis rides the client
// port instead of a separate bus), so auth is hot-applied after the join.
package e7cluster

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/linkerlin/godis/lib/hashslot"
)

// authedSend dials a fresh connection, AUTHs, then sends one command — the
// harness's mini client once requirepass is set.
func authedSend(t *testing.T, n *node, password string, args ...string) string {
	t.Helper()
	c, err := net.DialTimeout("tcp", n.addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", n.addr, err)
	}
	defer c.Close()
	cl := &rclient{conn: c, r: bufio.NewReader(c)}
	if r := cl.send("AUTH", password); !strings.HasPrefix(r, "+") {
		t.Fatalf("AUTH on %s: %s\nlogs:\n%s", n.addr, r, n.out.String())
	}
	return cl.send(args...)
}

// TestE7GoRedisClusterAuthAcl certifies the cluster-mode auth plane with a
// real client: every pooled ClusterClient connection authenticates against
// its node, and ACL users are enforced per node.
func TestE7GoRedisClusterAuthAcl(t *testing.T) {
	if testing.Short() {
		t.Skip("go-redis cluster auth e2e skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	waitSlotsQuiet(t, seed, joiner)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const clusterPw = "clusterpw-1"
	const appPw = "apppw-2"

	// hot-apply requirepass on both nodes
	for _, n := range []*node{seed, joiner} {
		if r := n.trySend("CONFIG", "SET", "requirepass", clusterPw); !strings.HasPrefix(r, "+") {
			t.Fatalf("CONFIG SET requirepass on %s: %s\nlogs:\n%s", n.addr, r, n.out.String())
		}
	}
	// the gate is live: unauthenticated commands NOAUTH, bad password WRONGPASS
	if r := seed.trySend("PING"); !strings.Contains(r, "NOAUTH") {
		t.Fatalf("unauthenticated PING = %q, want NOAUTH", r)
	}
	if r := joiner.trySend("GET", "x"); !strings.Contains(r, "NOAUTH") {
		t.Fatalf("unauthenticated GET on joiner = %q, want NOAUTH", r)
	}
	if r := seed.trySend("AUTH", "wrongpw"); !strings.Contains(r, "WRONGPASS") {
		t.Fatalf("wrong-password AUTH = %q, want WRONGPASS", r)
	}

	// credentialed ClusterClient: AUTH on every pooled connection, routing
	// across both halves. Slot-0 and slot-16383 keys pin that BOTH nodes
	// actually served the flow (random keys alone can concentrate).
	lowKey, highKey := "auth:low", "auth:high"
	for i := 0; i < 4096 && hashslot.Slot(lowKey) >= 8192; i++ {
		lowKey = fmt.Sprintf("auth:low:%d", i)
	}
	for i := 0; i < 4096 && hashslot.Slot(highKey) < 8192; i++ {
		highKey = fmt.Sprintf("auth:high:%d", i)
	}
	if hashslot.Slot(lowKey) >= 8192 || hashslot.Slot(highKey) < 8192 {
		t.Fatalf("failed to craft keys in both halves: %q=%d %q=%d",
			lowKey, hashslot.Slot(lowKey), highKey, hashslot.Slot(highKey))
	}

	rdb := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:       []string{seed.addr, joiner.addr},
		Password:    clusterPw,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second,
		PoolSize:    8,
		MaxRetries:  2,
	})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("credentialed ClusterClient Ping: %v", err)
	}
	if err := rdb.Set(ctx, lowKey, "L", 0).Err(); err != nil {
		t.Fatalf("SET %s (slot %d): %v", lowKey, hashslot.Slot(lowKey), err)
	}
	if err := rdb.Set(ctx, highKey, "H", 0).Err(); err != nil {
		t.Fatalf("SET %s (slot %d): %v", highKey, hashslot.Slot(highKey), err)
	}
	for i := 0; i < 60; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("auth:k%d", i), i, 0).Err(); err != nil {
			t.Fatalf("SET auth:k%d: %v", i, err)
		}
	}
	if v, err := rdb.Get(ctx, lowKey).Result(); err != nil || v != "L" {
		t.Fatalf("GET %s = %q, %v", lowKey, v, err)
	}
	if v, err := rdb.Get(ctx, highKey).Result(); err != nil || v != "H" {
		t.Fatalf("GET %s = %q, %v", highKey, v, err)
	}

	// a client WITHOUT the password must not get through
	anon := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:       []string{seed.addr, joiner.addr},
		DialTimeout: 3 * time.Second,
		MaxRetries:  -1,
	})
	defer func() { _ = anon.Close() }()
	if _, err := anon.Get(ctx, lowKey).Result(); err == nil || !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("unauthenticated ClusterClient GET = %v, want NOAUTH", err)
	}
	// wrong password must not get through
	bad := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:       []string{seed.addr, joiner.addr},
		Password:    "nope",
		DialTimeout: 3 * time.Second,
		MaxRetries:  -1,
	})
	defer func() { _ = bad.Close() }()
	if _, err := bad.Ping(ctx).Result(); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("wrong-password ClusterClient Ping = %v, want WRONGPASS", err)
	}

	// --- ACL: identical restricted user on both nodes, then a credentialed
	// ClusterClient whose allowed ops must work on BOTH halves and denied
	// ops must get NOPERM from whichever node owns the key
	for _, n := range []*node{seed, joiner} {
		r := authedSend(t, n, clusterPw, "ACL", "SETUSER", "appuser", "on", ">"+appPw, "~app:*", "+GET", "+SET", "+cluster")
		if !strings.HasPrefix(r, "+") {
			t.Fatalf("ACL SETUSER appuser on %s: %s\nlogs:\n%s", n.addr, r, n.out.String())
		}
	}
	app := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:       []string{seed.addr, joiner.addr},
		Username:    "appuser",
		Password:    appPw,
		DialTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second,
		PoolSize:    8,
		MaxRetries:  2,
	})
	defer func() { _ = app.Close() }()
	appLow, appHigh := "app:low", "app:high"
	for i := 0; i < 4096 && hashslot.Slot(appLow) >= 8192; i++ {
		appLow = fmt.Sprintf("app:low:%d", i)
	}
	for i := 0; i < 4096 && hashslot.Slot(appHigh) < 8192; i++ {
		appHigh = fmt.Sprintf("app:high:%d", i)
	}
	for i, k := range []string{appLow, appHigh} {
		if err := app.Set(ctx, k, strconv.Itoa(i), 0).Err(); err != nil {
			t.Fatalf("appuser SET %s: %v", k, err)
		}
		if v, err := app.Get(ctx, k).Result(); err != nil || v != strconv.Itoa(i) {
			t.Fatalf("appuser GET %s = %q, %v", k, v, err)
		}
	}
	// denied command (DEL not granted) and denied key pattern (granted cmd,
	// out-of-pattern key) — NOPERM from the owner node in both cases
	if err := app.Del(ctx, appLow).Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("appuser DEL = %v, want NOPERM", err)
	}
	if _, err := app.Get(ctx, "denied:key").Result(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("appuser GET denied:key = %v, want NOPERM", err)
	}
	// appuser with a wrong password is rejected too
	badApp := goredis.NewClusterClient(&goredis.ClusterOptions{
		Addrs:       []string{seed.addr, joiner.addr},
		Username:    "appuser",
		Password:    "wrongapppw",
		DialTimeout: 3 * time.Second,
		MaxRetries:  -1,
	})
	defer func() { _ = badApp.Close() }()
	if _, err := badApp.Ping(ctx).Result(); err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("appuser wrong-password Ping = %v, want WRONGPASS", err)
	}
}
