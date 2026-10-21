package database

// R5-21 HOTKEYS (8.6) and R5-22 HIMPORT (8.10).
//
// HOTKEYS is an honest Godis subset: per-key counters sampled on the command
// path (CPU = command microseconds attributed to each key, NET = summed
// argument bytes), not Redis's per-slot probabilistic structures. The metric
// definitions are documented in docs/COMPATIBILITY.md.
//
// HIMPORT stores session fieldsets per connection and writes plain hashes
// (compact-hash encoding is out of scope).

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// ---- HOTKEYS (8.6) ----

type hotkeysSession struct {
	active    atomic.Bool
	mu        sync.Mutex
	started   bool // ever started (GET works after STOP until RESET)
	trackCPU  bool
	trackNET  bool
	sample    int // sampling ratio; each command counted with p = 1/sample
	topK      int
	duration  time.Duration // 0 = until STOP
	startUnix int64         // unix ms
	cpuUS     map[string]int64
	netBytes  map[string]int64
	totalCPU  int64
	totalNet  int64
}

var hotkeys hotkeysSession

const maxHotkeyTrackedKeys = 10000

func addBoundedHotkey(m map[string]int64, k string, delta int64) {
	if _, exists := m[k]; exists || len(m) < maxHotkeyTrackedKeys {
		m[k] += delta
		return
	}
	var minKey string
	var minVal int64 = -1
	for mk, mv := range m {
		if minVal < 0 || mv < minVal {
			minKey = mk
			minVal = mv
		}
	}
	if minVal >= 0 && delta > minVal {
		delete(m, minKey)
		m[k] = delta
	}
}

// recordHotkeys attributes one executed command to its keys. Called from the
// normal command path only; ponytail: O(keys) map adds per command while
// tracking is active, no-op otherwise.
func recordHotkeys(cmdLine [][]byte, write, read []string, usec uint64) {
	if !hotkeys.active.Load() {
		return
	}
	hotkeys.mu.Lock()
	defer hotkeys.mu.Unlock()
	if !hotkeys.active.Load() {
		return
	}
	if hotkeys.duration > 0 && time.Since(time.UnixMilli(hotkeys.startUnix)) > hotkeys.duration {
		hotkeys.active.Store(false)
		return
	}
	if hotkeys.sample > 1 && rand.Intn(hotkeys.sample) != 0 {
		return
	}
	net := int64(0)
	for _, a := range cmdLine {
		net += int64(len(a))
	}
	for _, k := range append(append([]string(nil), write...), read...) {
		if hotkeys.trackCPU {
			addBoundedHotkey(hotkeys.cpuUS, k, int64(usec))
		}
		if hotkeys.trackNET {
			addBoundedHotkey(hotkeys.netBytes, k, net)
		}
	}
	if hotkeys.trackCPU {
		hotkeys.totalCPU += int64(usec)
	}
	if hotkeys.trackNET {
		hotkeys.totalNet += net
	}
}

func topKPairs(m map[string]int64, k int) []redis.Reply {
	type kv struct {
		key string
		v   int64
	}
	all := make([]kv, 0, len(m))
	for key, v := range m {
		all = append(all, kv{key, v})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].v != all[j].v {
			return all[i].v > all[j].v
		}
		return all[i].key < all[j].key
	})
	if k > 0 && len(all) > k {
		all = all[:k]
	}
	out := make([]redis.Reply, 0, len(all)*2)
	for _, e := range all {
		out = append(out, protocol.MakeBulkReply([]byte(e.key)), protocol.MakeIntReply(e.v))
	}
	return out
}

// execHotkeys HOTKEYS START|STOP|GET|RESET (8.6)
func execHotkeys(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hotkeys' command")
	}
	switch strings.ToUpper(string(args[0])) {
	case "START":
		return execHotkeysStart(db, args[1:])
	case "STOP":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'hotkeys|stop' command")
		}
		hotkeys.mu.Lock()
		defer hotkeys.mu.Unlock()
		if !hotkeys.active.Load() {
			return &protocol.NullBulkReply{}
		}
		hotkeys.active.Store(false)
		return protocol.MakeOkReply()
	case "GET":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'hotkeys|get' command")
		}
		hotkeys.mu.Lock()
		defer hotkeys.mu.Unlock()
		if !hotkeys.started {
			return protocol.MakeNullBulkReply()
		}
		durationMs := int64(0)
		if hotkeys.duration > 0 {
			durationMs = hotkeys.duration.Milliseconds()
		}
		m := protocol.MakeMapReply()
		m.Put("tracking-active", protocol.MakeIntReply(boolToInt64(hotkeys.active.Load())))
		m.Put("sample-ratio", protocol.MakeIntReply(int64(hotkeys.sample)))
		m.Put("selected-slots", protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(0), protocol.MakeIntReply(16383)}))
		m.Put("all-commands-all-slots-us", protocol.MakeIntReply(hotkeys.totalCPU))
		m.Put("net-bytes-all-commands-all-slots", protocol.MakeIntReply(hotkeys.totalNet))
		m.Put("collection-start-time-unix-ms", protocol.MakeIntReply(hotkeys.startUnix))
		m.Put("collection-duration-ms", protocol.MakeIntReply(durationMs))
		if hotkeys.trackCPU {
			// Godis accounting: the tracked per-key microseconds are the
			// whole process view here (no separate user/sys split).
			m.Put("total-cpu-time-user-ms", protocol.MakeIntReply(hotkeys.totalCPU/1000))
			m.Put("total-cpu-time-sys-ms", protocol.MakeIntReply(0))
			m.Put("by-cpu-time-us", protocol.MakeMultiRawReply(topKPairs(hotkeys.cpuUS, hotkeys.topK)))
		}
		if hotkeys.trackNET {
			m.Put("total-net-bytes", protocol.MakeIntReply(hotkeys.totalNet))
			m.Put("by-net-bytes", protocol.MakeMultiRawReply(topKPairs(hotkeys.netBytes, hotkeys.topK)))
		}
		return m
	case "RESET":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'hotkeys|reset' command")
		}
		hotkeys.mu.Lock()
		defer hotkeys.mu.Unlock()
		hotkeys.active.Store(false)
		hotkeys.started = false
		hotkeys.cpuUS = nil
		hotkeys.netBytes = nil
		hotkeys.totalCPU = 0
		hotkeys.totalNet = 0
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + strings.ToLower(string(args[0])) + "'. Try HOTKEYS HELP.")
	}
}

func execHotkeysStart(db *DB, args [][]byte) redis.Reply {
	trackCPU, trackNET := false, false
	metricsCount := -1
	topK := 10
	durationSec := 0
	sample := 1
	for i := 0; i < len(args); {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "METRICS":
			if metricsCount >= 0 || i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 1 {
				return protocol.MakeErrReply("ERR Invalid METRICS count")
			}
			metricsCount = n
			i += 2
			for i < len(args) {
				switch strings.ToUpper(string(args[i])) {
				case "CPU":
					trackCPU = true
					i++
				case "NET":
					trackNET = true
					i++
				default:
					// next outer option (COUNT/DURATION/…)
				}
				if i >= len(args) || !isMetricsToken(args[i]) {
					break
				}
			}
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 1 {
				return protocol.MakeErrReply("ERR Invalid COUNT")
			}
			topK = n
			i += 2
		case "DURATION":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR Invalid DURATION")
			}
			durationSec = n
			i += 2
		case "SAMPLE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 1 {
				return protocol.MakeErrReply("ERR Invalid SAMPLE")
			}
			sample = n
			i += 2
		case "SLOTS":
			// Cluster-only option; standalone Godis rejects it like Redis.
			return protocol.MakeErrReply("ERR SLOTS is supported only in cluster mode")
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	if metricsCount < 0 {
		return protocol.MakeSyntaxErrReply()
	}
	selected := 0
	if trackCPU {
		selected++
	}
	if trackNET {
		selected++
	}
	if selected == 0 {
		return protocol.MakeErrReply("ERR At least one of CPU or NET must be specified")
	}
	if selected != metricsCount {
		return protocol.MakeErrReply("ERR METRICS count must match the number of selected metrics")
	}

	hotkeys.mu.Lock()
	defer hotkeys.mu.Unlock()
	if hotkeys.active.Load() {
		return protocol.MakeErrReply("ERR Hotkeys tracking is already active")
	}
	hotkeys.trackCPU = trackCPU
	hotkeys.trackNET = trackNET
	hotkeys.sample = sample
	hotkeys.topK = topK
	hotkeys.duration = time.Duration(durationSec) * time.Second
	hotkeys.startUnix = time.Now().UnixMilli()
	hotkeys.cpuUS = make(map[string]int64)
	hotkeys.netBytes = make(map[string]int64)
	hotkeys.totalCPU = 0
	hotkeys.totalNet = 0
	hotkeys.started = true
	hotkeys.active.Store(true)
	return protocol.MakeOkReply()
}

func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func isMetricsToken(arg []byte) bool {
	u := strings.ToUpper(string(arg))
	return u == "CPU" || u == "NET"
}

// ---- HIMPORT (8.10) ----

// himportSessions maps client ID -> fieldset name -> ordered field names.
// ponytail: per-connection fieldsets live until RESET; there is no
// disconnect hook, so PREPARE caps fieldsets per connection at 128.
var (
	himportSessions sync.Map
	himportMu       sync.Mutex
)

const himportMaxFieldsetsPerConn = 128

func execHImportConn(c redis.Connection, db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'himport' command")
	}
	switch strings.ToUpper(string(args[0])) {
	case "PREPARE":
		return execHImportPrepare(c, db, args[1:])
	case "SET":
		return execHImportSet(c, db, args[1:])
	case "DISCARD":
		if c == nil {
			return protocol.MakeErrReply("ERR HIMPORT DISCARD requires a client connection")
		}
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'himport|discard' command")
		}
		himportMu.Lock()
		if fieldsets, ok := himportSessions.Load(c.GetClientID()); ok {
			delete(fieldsets.(map[string][]string), string(args[1]))
		}
		himportMu.Unlock()
		return protocol.MakeOkReply()
	case "DISCARDALL":
		if c == nil {
			return protocol.MakeErrReply("ERR HIMPORT DISCARDALL requires a client connection")
		}
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'himport|discardall' command")
		}
		himportMu.Lock()
		himportSessions.Delete(c.GetClientID())
		himportMu.Unlock()
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + strings.ToLower(string(args[0])) + "'. Try HIMPORT HELP.")
	}
}

// himportFieldsets returns (creating if needed) the connection's fieldset map.
func himportFieldsets(c redis.Connection) (map[string][]string, bool) {
	if c == nil {
		return nil, false
	}
	raw, _ := himportSessions.LoadOrStore(c.GetClientID(), map[string][]string{})
	return raw.(map[string][]string), true
}

// ClearHImportSession drops a connection's fieldsets (RESET path).
func ClearHImportSession(clientID int64) {
	himportMu.Lock()
	himportSessions.Delete(clientID)
	himportMu.Unlock()
}

func execHImportPrepare(c redis.Connection, db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'himport|prepare' command")
	}
	name := string(args[0])
	seen := make(map[string]struct{}, len(args)-1)
	fields := make([]string, 0, len(args)-1)
	for _, f := range args[1:] {
		fs := string(f)
		if _, dup := seen[fs]; dup {
			return protocol.MakeErrReply("ERR Field names must be unique within a fieldset")
		}
		seen[fs] = struct{}{}
		fields = append(fields, fs)
	}
	himportMu.Lock()
	defer himportMu.Unlock()
	fieldsets, ok := himportFieldsets(c)
	if !ok {
		return protocol.MakeErrReply("ERR HIMPORT PREPARE requires a client connection")
	}
	fieldsets[name] = fields
	if len(fieldsets) > himportMaxFieldsetsPerConn {
		// ponytail: bounded growth without a disconnect hook.
		delete(fieldsets, name)
		return protocol.MakeErrReply("ERR Too many fieldsets prepared on this connection")
	}
	return protocol.MakeOkReply()
}

func execHImportSet(c redis.Connection, db *DB, args [][]byte) redis.Reply {
	if c == nil {
		return protocol.MakeErrReply("ERR HIMPORT SET requires a client connection")
	}
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'himport|set' command")
	}
	key := string(args[0])
	name := string(args[1])
	himportMu.Lock()
	raw, ok := himportSessions.Load(c.GetClientID())
	if !ok {
		himportMu.Unlock()
		return protocol.MakeErrReply("ERR No such fieldset")
	}
	fields, ok := raw.(map[string][]string)[name]
	himportMu.Unlock()
	if !ok {
		return protocol.MakeErrReply("ERR No such fieldset")
	}
	values := args[2:]
	if len(values) != len(fields) {
		return protocol.MakeErrReply("ERR Value count does not match the fieldset")
	}

	// Build the plain hash via the HSET normal execution path (locking, WATCH
	// version bump, client-side caching invalidation, AOF, index feeding and
	// notifications all follow).
	hsetCmd := make([][]byte, 0, 2+len(fields)*2)
	hsetCmd = append(hsetCmd, []byte("HSET"), []byte(key))
	for i, f := range fields {
		hsetCmd = append(hsetCmd, []byte(f), values[i])
	}
	r := db.execNormalCommand(c, hsetCmd)
	if protocol.IsErrorReply(r) {
		return r
	}
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("Hotkeys", execHotkeys, prepareNoKeys, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
}
