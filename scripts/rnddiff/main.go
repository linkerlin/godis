// Command rnddiff is the E6-2 random differential tester: it generates
// type-aware random command sequences (deterministic for a given seed), sends
// each command to Godis and Redis 8.10, compares normalized replies, and
// checkpoints full dataset state (per-key TYPE + normalized dump — cross-
// engine DEBUG DIGEST is not comparable: the implementations hash different
// serializations). On divergence it greedily minimizes the prefix and writes
// a replayable file.
//
// Usage:
//	go run ./scripts/rnddiff --godis 127.0.0.1:16380 --redis 127.0.0.1:16379 \
//	  --steps 100000 --seed 42
//	go run ./scripts/rnddiff ... --replay tmp/rnddiff/fail-xxx.txt
//
// Entry point with sidecars: scripts/rnd-diff.sh
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	flagGodis = "127.0.0.1:16380"
	flagRedis = "127.0.0.1:16379"
)

type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(addr string) *client {
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial %s: %v\n", addr, err)
		os.Exit(2)
	}
	return &client{conn: c, r: bufio.NewReader(c)}
}

func (c *client) send(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		return "<IOERR " + err.Error() + ">"
	}
	return c.readReply()
}

// readReply returns a normalized text form: status "s:...", errors "-:...",
// integers "i:N", bulk "b:...", nil "n", arrays "a[...|...]".
func (c *client) readReply() string {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "<IOERR " + err.Error() + ">"
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "<BAD>"
	}
	switch line[0] {
	case '+', '-', ':':
		return string(line[0]) + ":" + line[1:]
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "n"
		}
		buf := make([]byte, n+2)
		if _, err := readFull(c.r, buf); err != nil {
			return "<IOERR>"
		}
		return "b:" + string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "n"
		}
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			parts[i] = c.readReply()
		}
		return "a[" + strings.Join(parts, "|") + "]"
	}
	return "<BAD " + line + ">"
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// bulkArray extracts the string values from a normalized array reply.
func bulkArray(reply string) []string {
	if !strings.HasPrefix(reply, "a[") {
		return nil
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(reply, "a["), "]")
	if inner == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(inner, "|") {
		if strings.HasPrefix(p, "b:") {
			out = append(out, strings.TrimPrefix(p, "b:"))
		}
	}
	return out
}

// ---- generator state (client-side model; guides valid command emission) ----

const keyPrefix = "k:"
const maxKeys = 300

var fieldPool = []string{"f0", "f1", "f2", "f3", "f4", "f5"}
var memberPool = []string{"m0", "m1", "m2", "m3", "m4", "m5"}

type keyInfo struct {
	typ       string
	isInt     bool
	listLen   int
	fields    map[string]bool
	fieldInt  map[string]bool
	members   map[string]bool
	zmembers  map[string]bool
}

type model struct {
	r     *rand.Rand
	names []string // ordered so generation is reproducible for a seed
	keys  map[string]*keyInfo
}

func newModel(seed int64) *model {
	return &model{r: rand.New(rand.NewSource(seed)), keys: map[string]*keyInfo{}}
}

func (m *model) addKey(name string, ki *keyInfo) {
	m.keys[name] = ki
	m.names = append(m.names, name)
}

func (m *model) dropKey(name string) {
	delete(m.keys, name)
	for i, n := range m.names {
		if n == name {
			m.names = append(m.names[:i], m.names[i+1:]...)
			break
		}
	}
}

func (m *model) pick() (string, *keyInfo) {
	if len(m.names) == 0 {
		return "", nil
	}
	name := m.names[m.r.Intn(len(m.names))]
	return name, m.keys[name]
}

func (m *model) pickByType(t string) (string, bool) {
	var cands []string
	for _, n := range m.names {
		if m.keys[n].typ == t {
			cands = append(cands, n)
		}
	}
	if len(cands) == 0 {
		return "", false
	}
	return cands[m.r.Intn(len(cands))], true
}

func (m *model) randStr() string    { return "v" + strconv.Itoa(m.r.Intn(10000)) }
func (m *model) randIntStr() string { return strconv.Itoa(m.r.Intn(100000) - 50000) }
func (m *model) newKey() string {
	return fmt.Sprintf("%st%dr%d", keyPrefix, m.r.Intn(1<<30), len(m.names))
}

func (m *model) gen() []string {
	if len(m.names) < maxKeys && m.r.Float64() < 0.15 {
		name := m.newKey()
		switch m.r.Intn(5) {
		case 0:
			m.addKey(name, &keyInfo{typ: "string"})
			return []string{"SET", name, m.randStr()}
		case 1:
			m.addKey(name, &keyInfo{typ: "string", isInt: true})
			return []string{"SET", name, m.randIntStr()}
		case 2:
			m.addKey(name, &keyInfo{typ: "hash", fields: map[string]bool{}, fieldInt: map[string]bool{}})
			return []string{"HSET", name, fieldPool[0], m.randStr()}
		case 3:
			m.addKey(name, &keyInfo{typ: "list", listLen: 1})
			return []string{"LPUSH", name, m.randStr()}
		default:
			m.addKey(name, &keyInfo{typ: "zset", zmembers: map[string]bool{memberPool[0]: true}})
			return []string{"ZADD", name, m.randIntStr(), memberPool[0]}
		}
	}

	name, ki := m.pick()
	if name == "" {
		return []string{"PING"}
	}
	switch ki.typ {
	case "string":
		switch m.r.Intn(10) {
		case 0:
			ki.isInt = false
			return []string{"SET", name, m.randStr()}
		case 1:
			return []string{"GET", name}
		case 2:
			ki.isInt = false
			return []string{"APPEND", name, m.randStr()}
		case 3:
			return []string{"STRLEN", name}
		case 4:
			return []string{"GETRANGE", name, strconv.Itoa(m.r.Intn(50)-10), strconv.Itoa(m.r.Intn(60)-5)}
		case 5:
			ki.isInt = false
			return []string{"SETRANGE", name, strconv.Itoa(m.r.Intn(30)), m.randStr()}
		case 6:
			if ki.isInt {
				return []string{"INCRBY", name, strconv.Itoa(m.r.Intn(2001)-1000)}
			}
			ki.isInt = true
			return []string{"SET", name, m.randIntStr()}
		case 7:
			if ki.isInt {
				return []string{"DECR", name}
			}
			return []string{"GET", name}
		case 8:
			ki.isInt = false
			return []string{"GETSET", name, m.randStr()}
		default:
			return []string{"TYPE", name}
		}
	case "hash":
		f := fieldPool[m.r.Intn(len(fieldPool))]
		switch m.r.Intn(8) {
		case 0:
			ki.fields[f] = true
			ki.fieldInt[f] = false
			return []string{"HSET", name, f, m.randStr()}
		case 1:
			return []string{"HGET", name, f}
		case 2:
			return []string{"HEXISTS", name, f}
		case 3:
			return []string{"HLEN", name}
		case 4:
			return []string{"HSTRLEN", name, f}
		case 5:
			if ki.fields[f] {
				delete(ki.fields, f)
				delete(ki.fieldInt, f)
				return []string{"HDEL", name, f}
			}
			return []string{"HGET", name, f}
		case 6:
			if ki.fieldInt[f] {
				return []string{"HINCRBY", name, f, strconv.Itoa(m.r.Intn(101) - 50)}
			}
			return []string{"HSET", name, f, m.randStr()}
		default:
			return []string{"HMGET", name, fieldPool[m.r.Intn(3)], fieldPool[m.r.Intn(3)]}
		}
	case "list":
		switch m.r.Intn(10) {
		case 0:
			ki.listLen++
			return []string{"LPUSH", name, m.randStr()}
		case 1:
			ki.listLen++
			return []string{"RPUSH", name, m.randStr()}
		case 2:
			if ki.listLen > 0 {
				ki.listLen--
				return []string{"LPOP", name}
			}
			return []string{"LLEN", name}
		case 3:
			if ki.listLen > 0 {
				ki.listLen--
				return []string{"RPOP", name}
			}
			return []string{"LLEN", name}
		case 4:
			return []string{"LLEN", name}
		case 5:
			if ki.listLen > 0 {
				return []string{"LINDEX", name, strconv.Itoa(m.r.Intn(ki.listLen))}
			}
			return []string{"LINDEX", name, "0"}
		case 6:
			return []string{"LRANGE", name, strconv.Itoa(m.r.Intn(10)-2), strconv.Itoa(m.r.Intn(20)-2)}
		case 7:
			if ki.listLen > 0 {
				return []string{"LSET", name, strconv.Itoa(m.r.Intn(ki.listLen)), m.randStr()}
			}
			return []string{"LLEN", name}
		case 8:
			if other, ok := m.pickByType("list"); ok && other != name && ki.listLen > 0 {
				return []string{"RPOPLPUSH", name, other}
			}
			return []string{"LLEN", name}
		default:
			if ki.listLen > 0 {
				return []string{"LTRIM", name, "0", strconv.Itoa(m.r.Intn(ki.listLen))}
			}
			return []string{"LLEN", name}
		}
	case "set":
		member := memberPool[m.r.Intn(len(memberPool))]
		switch m.r.Intn(6) {
		case 0:
			ki.members[member] = true
			return []string{"SADD", name, member}
		case 1:
			if ki.members[member] {
				delete(ki.members, member)
				return []string{"SREM", name, member}
			}
			return []string{"SCARD", name}
		case 2:
			return []string{"SCARD", name}
		case 3:
			return []string{"SISMEMBER", name, member}
		case 4:
			return []string{"SMISMEMBER", name, memberPool[m.r.Intn(3)], memberPool[m.r.Intn(3)]}
		default:
			if other, ok := m.pickByType("set"); ok && other != name {
				return []string{"SMOVE", name, other, member}
			}
			return []string{"SCARD", name}
		}
	default: // zset
		member := memberPool[m.r.Intn(len(memberPool))]
		switch m.r.Intn(10) {
		case 0:
			ki.zmembers[member] = true
			return []string{"ZADD", name, m.randIntStr(), member}
		case 1:
			return []string{"ZSCORE", name, member}
		case 2:
			return []string{"ZCARD", name}
		case 3:
			return []string{"ZRANK", name, member}
		case 4:
			return []string{"ZREVRANK", name, member}
		case 5:
			return []string{"ZRANGE", name, "0", "-1"}
		case 6:
			return []string{"ZRANGEBYSCORE", name, "(" + m.randIntStr(), "+"}
		case 7:
			if ki.zmembers[member] {
				delete(ki.zmembers, member)
				return []string{"ZREM", name, member}
			}
			return []string{"ZCARD", name}
		case 8:
			return []string{"ZPOPMIN", name}
		default:
			return []string{"ZINCRBY", name, m.randIntStr(), member}
		}
	}
}

// gen2 emits cross-type commands (DEL/EXISTS/RENAME/EXPIRE/PERSIST).
func (m *model) gen2() []string {
	name, ki := m.pick()
	if name == "" {
		return []string{"DBSIZE"}
	}
	switch m.r.Intn(6) {
	case 0:
		m.dropKey(name)
		return []string{"DEL", name}
	case 1:
		return []string{"EXISTS", name, name}
	case 2:
		return []string{"TYPE", name}
	case 3:
		dst := m.newKey()
		m.dropKey(name)
		m.addKey(dst, ki)
		return []string{"RENAME", name, dst}
	case 4:
		return []string{"EXPIRE", name, "100000"} // never expires mid-run
	default:
		return []string{"PERSIST", name}
	}
}

// ---- state checkpoint: per-key TYPE + normalized dump, sorted ----

func stateDump(c *client) string {
	keys := bulkArray(c.send("KEYS", keyPrefix+"*"))
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		t := strings.TrimPrefix(c.send("TYPE", k), "s:")
		var dump string
		switch t {
		case "string":
			dump = c.send("GET", k)
		case "hash":
			dump = sortLines(c.send("HGETALL", k))
		case "list":
			dump = c.send("LRANGE", k, "0", "-1")
		case "set":
			dump = sortLines(c.send("SMEMBERS", k))
		case "zset":
			dump = c.send("ZRANGE", k, "0", "-1", "WITHSCORES")
		}
		out = append(out, k+" => "+t+" "+dump)
	}
	return strings.Join(out, "\n")
}

func sortLines(reply string) string {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(reply, "a["), "]"), "|")
	sort.Strings(parts)
	return "a[" + strings.Join(parts, "|") + "]"
}

func main() {
	var (
		godisAddr = flag.String("godis", flagGodis, "godis host:port")
		redisAddr = flag.String("redis", flagRedis, "redis host:port")
		steps     = flag.Int("steps", 100000, "total commands to send")
		seed      = flag.Int64("seed", 42, "rng seed")
		checkAt   = flag.Int("check-every", 2000, "state checkpoint interval")
		replay    = flag.String("replay", "", "replay a saved divergence file instead of generating")
		outDir    = flag.String("out", "tmp/rnddiff", "directory for failure artifacts")
	)
	flag.Parse()
	flagGodis, flagRedis = *godisAddr, *redisAddr

	g := dial(*godisAddr)
	r := dial(*redisAddr)
	g.send("FLUSHALL")
	r.send("FLUSHALL")

	if *replay != "" {
		runReplay(g, r, *replay)
		return
	}

	m := newModel(*seed)
	_ = os.MkdirAll(*outDir, 0o755)
	var history [][]string
	divergences := 0

	for i := 1; i <= *steps; i++ {
		var cmd []string
		if m.r.Float64() < 0.12 {
			cmd = m.gen2()
		} else {
			cmd = m.gen()
		}
		history = append(history, cmd)
		gr, rr := g.send(cmd...), r.send(cmd...)
		if gr != rr {
			divergences++
			fmt.Printf("DIVERGENCE step %d: %v\n  godis: %s\n  redis: %s\n", i, cmd, gr, rr)
			writeFailure(*outDir, history, cmd, gr, rr)
			break
		}
		if i%*checkAt == 0 {
			gs, rs := stateDump(g), stateDump(r)
			if gs != rs {
				divergences++
				fmt.Printf("STATE DIVERGENCE at checkpoint step %d\n", i)
				os.WriteFile(fmt.Sprintf("%s/state-%d.txt", *outDir, time.Now().UnixNano()),
					[]byte("GODIS:\n"+gs+"\n\nREDIS:\n"+rs), 0o644)
				break
			}
		}
	}
	fmt.Printf("rnddiff done: steps=%d seed=%d divergences=%d\n", len(history), *seed, divergences)
	if divergences > 0 {
		os.Exit(1)
	}
}

func runReplay(g, r *client, path string) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer f.Close()
	g.send("FLUSHALL")
	r.send("FLUSHALL")
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	n := 0
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		cmd := strings.Split(line, "\x1f")
		gr, rr := g.send(cmd...), r.send(cmd...)
		n++
		if gr != rr {
			fmt.Printf("cmd %d %v\n  godis: %s\n  redis: %s\n", n, cmd, gr, rr)
		}
	}
	fmt.Printf("replay done: %d commands\n", n)
}

// writeFailure greedily minimizes the trailing command window while the same
// mismatch on the final command still reproduces, then saves a replay file.
// ponytail: fixed 300-step window and 90s budget; full ddmin over 100k-step
// histories would cost hours for little extra shrink.
func writeFailure(outDir string, history [][]string, cmd []string, gr, rr string) {
	window := history
	if len(window) > 300 {
		window = append([][]string{}, window[len(window)-300:]...)
	}
	deadline := time.Now().Add(90 * time.Second)
	for j := 0; j < len(window)-1 && time.Now().Before(deadline); j++ {
		trial := make([][]string, 0, len(window)-1)
		trial = append(trial, window[:j]...)
		trial = append(trial, window[j+1:]...)
		g, r := dial(flagGodis), dial(flagRedis)
		g.send("FLUSHALL")
		r.send("FLUSHALL")
		for _, c := range trial {
			g.send(c...)
			r.send(c...)
		}
		gr2, rr2 := g.send(cmd...), r.send(cmd...)
		g.conn.Close()
		r.conn.Close()
		if gr2 != rr2 {
			window = trial // drop succeeded; indices shifted, do not advance j
		}
	}
	name := fmt.Sprintf("%s/fail-%d.txt", outDir, time.Now().UnixNano())
	f, err := os.Create(name)
	if err != nil {
		fmt.Println("cannot write failure file:", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "# minimized divergence (args separated by \\x1f; replays via --replay)\n")
	fmt.Fprintf(f, "# failing cmd: %v\n# godis: %s\n# redis: %s\n", cmd, gr, rr)
	for _, c := range window {
		f.WriteString(strings.Join(c, "\x1f") + "\n")
	}
	fmt.Println("minimized case ->", name)
}
