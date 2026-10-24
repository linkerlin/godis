// Retry-contract e2e cases: the redirect retry paths every real cluster
// client (redis-cli -c, go-redis, lettuce) implements, drilled against a
// live two-node cluster.
//
//   - TestE7ClusterAskRetryContract: mid-reshard -ASK shape, ASKING
//     one-shot gating on the importer, importer bounce-back, write
//     redirects for missing keys, and stale-owner -MOVED convergence after
//     the reshard finishes.
//   - TestE7ClusterMovedRetryLoop: every command deliberately sent to the
//     wrong node of a balanced ring must converge in exactly one -MOVED hop.
//
// Both wait out the join-time auto-rebalance first (clusterCron evens out
// slot ownership after a join; mid-flight slots answer -ASK from their
// owner, which is legal but not what these tests pin down).
package e7cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/hashslot"
)

// bootTwoNodes starts a seed + joiner pair and waits for the join to
// propagate. Slot ownership is still in flux when it returns.
func bootTwoNodes(t *testing.T) (*node, *node) {
	t.Helper()
	dirA, dirB := t.TempDir(), t.TempDir()
	seed := startNode(t, "seed", dirA, "cluster-as-seed yes")
	joiner := startNode(t, "joiner", dirB, "cluster-seed "+seed.addr)
	deadline := time.Now().Add(30 * time.Second)
	for {
		r := joiner.trySend("CLUSTER", "NODES")
		if len(r) > 20 && !strings.Contains(r, "IOERR") {
			return seed, joiner
		}
		if time.Now().After(deadline) {
			t.Fatalf("joiner never joined\nseed logs:\n%s\njoiner logs:\n%s", seed.out.String(), joiner.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// splitRing carves slots 8192-16383 for the joiner and waits until both
// nodes see one half each (no data involved, so del+add is enough; each
// step waits for FSM propagation before the next). Returns the slot map.
func splitRing(t *testing.T, seed, joiner *node) []slotRange {
	t.Helper()
	if r := seed.trySend("CLUSTER", "DELSLOTSRANGE", "8192", "16383"); strings.HasPrefix(r, "-") {
		t.Fatalf("DELSLOTSRANGE on seed: %s\nlogs:\n%s", r, seed.out.String())
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		flat := joiner.trySend("CLUSTER", "SLOTS")
		if rs := parseSlots(t, flat); len(rs) == 1 {
			if o, ok := ownerOf(rs, 8192); !ok || o == "" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("upper half still assigned: %q\nseed logs:\n%s", flat, seed.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	var addOK bool
	deadline = time.Now().Add(30 * time.Second)
	for !addOK {
		if r := joiner.trySend("CLUSTER", "ADDSLOTSRANGE", "8192", "16383"); !strings.HasPrefix(r, "-") {
			addOK = true
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ADDSLOTSRANGE on joiner kept failing\nlogs:\n%s", joiner.out.String())
		}
		time.Sleep(500 * time.Millisecond)
	}
	var ranges []slotRange
	deadline = time.Now().Add(30 * time.Second)
	for {
		flat := joiner.trySend("CLUSTER", "SLOTS")
		ranges = parseSlots(t, flat)
		if len(ranges) == 2 {
			o1, _ := ownerOf(ranges, 1)
			o2, _ := ownerOf(ranges, 16383)
			if (o1 == seed.addr && o2 == joiner.addr) || (o1 == joiner.addr && o2 == seed.addr) {
				t.Logf("topology ready: slots 0-8191 -> %s, 8192-16383 -> %s (seed=%s joiner=%s)",
					o1, o2, seed.addr, joiner.addr)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topology incomplete: slots=%q (seed=%s joiner=%s)\nseed logs:\n%s\njoiner logs:\n%s",
				flat, seed.addr, joiner.addr, seed.out.String(), joiner.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	return ranges
}

// waitSlotsQuiet waits out the join-time auto-rebalance and returns the
// settled slot map: both nodes agree on exactly two contiguous ranges, the
// map has not changed for several samples, and a probe write+read through
// each range's owner succeeds without redirects.
func waitSlotsQuiet(t *testing.T, seed, joiner *node) []slotRange {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	var prev string
	same := 0
	var ranges []slotRange
	for {
		flat := joiner.trySend("CLUSTER", "SLOTS")
		ranges = parseSlots(t, flat)
		if flat == prev && len(ranges) == 2 {
			same++
		} else {
			same, prev = 0, flat
		}
		if same >= 8 && seed.trySend("CLUSTER", "SLOTS") == flat {
			ok := true
			for _, r := range ranges {
				owner := fmt.Sprintf("%s:%d", r.host, r.port)
				probe := &node{addr: owner}
				var key string
				for i := 0; ; i++ { // a probe key that lands inside r
					key = fmt.Sprintf("quiet:%d:%d", r.start, i)
					if s := int(hashslot.Slot(key)); s >= r.start && s <= r.end {
						break
					}
				}
				if x := probe.trySend("SET", key, "1"); x != "+OK" {
					ok = false
					break
				}
				if x := probe.trySend("GET", key); x != "1" {
					ok = false
					break
				}
			}
			if ok {
				t.Logf("slots quiet: %s", flat)
				return ranges
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot map never settled: %q\nseed logs:\n%s\njoiner logs:\n%s", flat, seed.out.String(), joiner.out.String())
		}
		time.Sleep(400 * time.Millisecond)
	}
}

// movedRetry is the dumb-client retry loop: send to addr, follow -MOVED
// redirects only (no topology refresh), and report the reply, the first
// reply, and how many attempts it took.
func movedRetry(clients map[string]*rclient, addr string, args ...string) (reply, first string, attempts int) {
	attempts = 1
	r := clients[addr].send(args...)
	first = r
	for strings.HasPrefix(r, "-MOVED ") && attempts < 5 {
		addr = strings.Fields(r)[2]
		next := clients[addr]
		if next == nil {
			return fmt.Sprintf("<unknown MOVED target %q>", addr), first, attempts
		}
		r = next.send(args...)
		attempts++
	}
	return r, first, attempts
}

// TestE7ClusterAskRetryContract drills the client retry contract in the
// half-open migration window (MIGRATING/IMPORTING markers set, only part of
// the slot migrated) and after the reshard finishes. The exporter is
// whoever owns the slot once the join rebalance settles.
func TestE7ClusterAskRetryContract(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster retry contract skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	ranges := waitSlotsQuiet(t, seed, joiner)

	seedC, joinerC := seed.client(t), joiner.client(t)
	defer seedC.close()
	defer joinerC.close()
	clients := map[string]*rclient{seed.addr: seedC, joiner.addr: joinerC}
	other := func(addr string) string {
		if addr == seed.addr {
			return joiner.addr
		}
		return seed.addr
	}

	// 12 keys in one hash-tag slot so a single reshard covers them all
	const n = 12
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("{askrs}%d", i)
	}
	slot := int(hashslot.Slot(keys[0]))
	exporter, _ := ownerOf(ranges, slot)
	importer := other(exporter)
	expC, impC := clients[exporter], clients[importer]
	t.Logf("slot %d (%d keys) mid-reshard retry drill: %s -> %s (seed=%s joiner=%s)",
		slot, n, exporter, importer, seed.addr, joiner.addr)

	for i, k := range keys {
		if r := expC.send("SET", k, "v"+strconv.Itoa(i)); r != "+OK" {
			t.Fatalf("SET %s via owner: %s", k, r)
		}
	}

	// reshard markers exactly like redis-cli --cluster (node id == addr)
	if r := expC.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "MIGRATING", importer); r != "+OK" {
		t.Fatalf("SETSLOT MIGRATING: %s", r)
	}
	if r := impC.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "IMPORTING", exporter); r != "+OK" {
		t.Fatalf("SETSLOT IMPORTING: %s", r)
	}
	host, port, _ := net.SplitHostPort(importer)
	migrate := func(ks []string) {
		t.Helper()
		args := append([]string{"MIGRATE", host, port, "", "0", "5000", "KEYS"}, ks...)
		if r := expC.send(args...); r != "+OK" {
			t.Fatalf("MIGRATE %d keys: %s\nseed logs:\n%s\njoiner logs:\n%s", len(ks), r, seed.out.String(), joiner.out.String())
		}
	}
	// gate probe: ASKING + a missing key of the importing slot must serve
	// locally (nil), not bounce — isolates the ASKING gate before any data
	// moves
	if r := impC.send("ASKING"); r != "+OK" {
		t.Fatalf("ASKING probe: %s", r)
	}
	if r := impC.send("GET", "{askrs}probe"); r != "<nil>" {
		t.Fatalf("ASKING+GET probe via importer = %q, want <nil> (gate open)\nseed logs:\n%s\njoiner logs:\n%s",
			r, seed.out.String(), joiner.out.String())
	}
	// leave the window half-open: only the first half moved
	migrate(keys[:n/2])

	wantAsk := fmt.Sprintf("-ASK %d %s", slot, importer)
	wantMoved := fmt.Sprintf("-MOVED %d %s", slot, exporter)
	askingGet := func(key, want string) {
		t.Helper()
		if r := impC.send("ASKING"); r != "+OK" {
			t.Fatalf("ASKING: %s", r)
		}
		if r := impC.send("GET", key); r != want {
			t.Fatalf("ASKING+GET %s via importer = %q, want %q", key, r, want)
		}
	}

	// migrated key on the exporter: exact -ASK shape, then the client
	// follows it — ASKING + retry at the importer serves the value
	if r := expC.send("GET", keys[0]); r != wantAsk {
		t.Fatalf("GET migrated key via exporter = %q, want %q", r, wantAsk)
	}
	askingGet(keys[0], "v0")
	// unmigrated key still serves on the exporter, no redirect
	if r := expC.send("GET", keys[n-1]); r != "v"+strconv.Itoa(n-1) {
		t.Fatalf("GET unmigrated key via exporter = %q, want v%d", r, n-1)
	}
	// importer without ASKING bounces back to the owner
	if r := impC.send("GET", keys[0]); r != wantMoved {
		t.Fatalf("GET via importer without ASKING = %q, want %q", r, wantMoved)
	}
	// writes redirect too: a missing key in a migrating slot is -ASK even
	// for SET; the client lands it on the importer
	if r := expC.send("SET", "{askrs}new", "fresh"); r != wantAsk {
		t.Fatalf("SET fresh key via exporter = %q, want %q", r, wantAsk)
	}
	if r := impC.send("ASKING"); r != "+OK" {
		t.Fatalf("ASKING: %s", r)
	}
	if r := impC.send("SET", "{askrs}new", "fresh"); r != "+OK" {
		t.Fatalf("ASKING+SET fresh key via importer = %q", r)
	}
	askingGet("{askrs}new", "fresh")
	// ASKING is one-shot: the very next command on the connection bounces
	if r := impC.send("ASKING"); r != "+OK" {
		t.Fatalf("ASKING: %s", r)
	}
	if r := impC.send("GET", keys[0]); r != "v0" {
		t.Fatalf("first command after ASKING = %q, want v0", r)
	}
	if r := impC.send("GET", keys[0]); r != wantMoved {
		t.Fatalf("second command after ASKING = %q, want one-shot bounce %q", r, wantMoved)
	}
	// exporter still lists exactly the unmigrated half
	if got := len(strings.Fields(expC.send("CLUSTER", "GETKEYSINSLOT", strconv.Itoa(slot), "100"))); got != n/2 {
		t.Fatalf("GETKEYSINSLOT mid-window lists %d keys, want %d", got, n/2)
	}

	// orphan guard: handing the slot over while keys remain is rejected
	if r := expC.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", importer); !strings.Contains(r, "still hold keys") {
		t.Fatalf("SETSLOT NODE with residual keys = %q, want orphan-guard rejection", r)
	}
	// finish: migrate the rest, then flip ownership on both ends
	migrate(keys[n/2:])
	if r := expC.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", importer); r != "+OK" {
		t.Fatalf("SETSLOT NODE (exporter): %s", r)
	}
	if r := impC.send("CLUSTER", "SETSLOT", strconv.Itoa(slot), "NODE", importer); r != "+OK" {
		t.Fatalf("SETSLOT NODE (importer): %s", r)
	}

	// stale client pinned to the old owner: wait for the FSM flip, then
	// every key must converge through exactly one -MOVED hop to the importer
	wantMovedNew := fmt.Sprintf("-MOVED %d %s", slot, importer)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if r := expC.send("GET", keys[0]); r == wantMovedNew {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("exporter never flipped to MOVED\nseed logs:\n%s", seed.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	wantVal := func(k string) string {
		if k == "{askrs}new" {
			return "fresh"
		}
		return "v" + strings.TrimPrefix(k, "{askrs}")
	}
	for _, k := range append(append([]string(nil), keys...), "{askrs}new") {
		r, first, hops := movedRetry(clients, exporter, "GET", k)
		if r != wantVal(k) || first != wantMovedNew || hops != 2 {
			t.Fatalf("stale-owner GET %s = %q (first %q) in %d hops, want %q first %q in 2",
				k, r, first, hops, wantVal(k), wantMovedNew)
		}
	}
}

// TestE7ClusterMovedRetryLoop drives every command at the WRONG node of the
// balanced two-node cluster: each must come back as an exact -MOVED to the
// owner and converge on the retry — the loop a client with a stale slot map
// runs on every command.
func TestE7ClusterMovedRetryLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster retry loop skipped in short mode")
	}
	seed, joiner := bootTwoNodes(t)
	ranges := waitSlotsQuiet(t, seed, joiner)

	seedC, joinerC := seed.client(t), joiner.client(t)
	defer seedC.close()
	defer joinerC.close()
	clients := map[string]*rclient{seed.addr: seedC, joiner.addr: joinerC}
	other := func(addr string) string {
		if addr == seed.addr {
			return joiner.addr
		}
		return seed.addr
	}

	// 30 keys per half; every op below goes to the wrong node first
	keysIn := func(r slotRange) []string {
		var ks []string
		for i := 0; len(ks) < 30; i++ {
			k := fmt.Sprintf("mv:%d", i)
			if s := int(hashslot.Slot(k)); s >= r.start && s <= r.end {
				ks = append(ks, k)
			}
		}
		return ks
	}
	for ri, r := range ranges {
		val := func(k string) string { return fmt.Sprintf("r%d:%s", ri, k) }
		for _, k := range keysIn(r) {
			owner, _ := ownerOf(ranges, int(hashslot.Slot(k)))
			wrong := other(owner)
			wantMoved := fmt.Sprintf("-MOVED %d %s", int(hashslot.Slot(k)), owner)
			if reply, first, hops := movedRetry(clients, wrong, "SET", k, val(k)); reply != "+OK" || first != wantMoved || hops != 2 {
				t.Fatalf("wrong-node SET %s via %s = %q (first %q) in %d hops, want +OK first %q in 2",
					k, wrong, reply, first, hops, wantMoved)
			}
		}
		for _, k := range keysIn(r) {
			owner, _ := ownerOf(ranges, int(hashslot.Slot(k)))
			wrong := other(owner)
			if reply, _, hops := movedRetry(clients, wrong, "GET", k); reply != val(k) || hops != 2 {
				t.Fatalf("wrong-node GET %s via %s = %q in %d hops, want %q in 2", k, wrong, reply, hops, val(k))
			}
		}
	}
}
