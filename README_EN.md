# Godis

![license](https://img.shields.io/github/license/linkerlin/godis)
[![Build Status](https://github.com/linkerlin/godis/actions/workflows/coverall.yml/badge.svg)](https://github.com/linkerlin/godis/actions?query=branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/linkerlin/godis/badge.svg?branch=master)](https://coveralls.io/github/linkerlin/godis?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/linkerlin/godis)](https://goreportcard.com/report/github.com/linkerlin/godis)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge-flat.svg)](https://github.com/avelino/awesome-go)

**[中文](./README.md)** | **English**

---

## What Actually Happens in the Millisecond After You Hit Enter?

Open a terminal, connect to port `6399`, and type:

```text
SET greeting "hello"
```

You hit Enter, and `OK` pops up immediately. What actually happened in between?

There is no magic. Your client wrapped those words into a plain stream of bytes—`*3\r\n$3\r\nSET\r\n$8\r\ngreeting\r\n$5\r\nhello\r\n`—and pushed them down a TCP socket. The server sliced those bytes apart, ran an FNV-32 hash on `"greeting"`, walked up to a wall of 65,536 numbered drawers (shards guarded by read-write locks), pulled open the right drawer, dropped the bytes inside, scribbled a note onto an append-only log file (AOF), and sent five bytes back down the wire: `+OK\r\n`.

That's all there is to it.

So why go through the trouble of building a **Redis 8.10–targeted** server from scratch in Go?

Because **knowing the name of something and actually figuring out how it works are two completely different things**. You can memorize terms like "skiplist", "time wheel", "RESP3 client-side caching", "HNSW vector graph", "Stream PEL", or "Raft consensus" in ten programming languages and draw neat boxes on a whiteboard—and still know nothing about how the machine actually ticks. You only really understand it when you build the gears yourself in code, hook up the real `redis-cli`, run over twelve thousand test cases against it, and watch it hold up.

**Godis** is that machine with the cover taken off, built so you can see every gear turn.

---

## Don't Take My Word for It — Try 5 Experiments in 30 Seconds

Arguments on paper are cheap; a 10-second demonstration beats a hundred pages of claims. Let's spin it up and poke at it.

### 1. Start the Server

Grab a prebuilt binary from [GitHub Releases](https://github.com/linkerlin/godis/releases), or build it straight from source:

```bash
go build -o godis ./
./godis
```

By default, Godis listens on `0.0.0.0:6399` (deliberately avoiding `6379` so it won't collide with a local Redis instance you might already be running). To pass a config file:

```bash
CONFIG=example.conf ./godis
# or
./godis -c example.conf -p 6399
```

Docker works too (the image runs as a non-root user; `/data` is the working/log volume):

```bash
docker build -t godis .
docker run --rm -p 6399:6399 -v godis-data:/data godis
# or
docker compose up -d
```

Mount a custom config at `/etc/godis/godis.conf`:

```bash
docker run --rm -p 6399:6399 \
  -v "$PWD/my.conf:/etc/godis/godis.conf:ro" \
  -v godis-data:/data godis
```

### 2. Five 10-Second Terminal Demos

Connect with `redis-cli -p 6399` and try these out. Each one strips the jargon off a modern Redis 8.x mechanism:

#### Experiment 1: Watch Client-Side Caching Catch a Mutation Live (RESP3 Push)
If a client caches a key locally, how does it know when someone else changes it without polling? Open two terminals:

```text
# Terminal A: Switch to RESP3, turn on tracking, and read 'price'
127.0.0.1:6399> HELLO 3
127.0.0.1:6399> CLIENT TRACKING ON
OK
127.0.0.1:6399> GET price
"100"

# Terminal B: Quietly change 'price'
127.0.0.1:6399> SET price 99
OK
```
Look back at **Terminal A**—without you typing a thing, the server pushes an invalidation frame (`>2\r\n+invalidate\r\n*1\r\n$5\r\nprice\r\n`) straight down the socket. The server simply remembers who read which key and taps them on the shoulder when that key is written.

#### Experiment 2: Put a Countdown Timer on a Single Hash Field (Redis 8.x `HEXPIRE`)
Previously, if you wanted an OTP code inside a user session hash to expire in 5 seconds, you had to expire the whole hash or split it into a separate string key. Now look:

```text
127.0.0.1:6399> HSET session:42 user "alice" otp "8848"
(integer) 2
127.0.0.1:6399> HEXPIRE session:42 5 FIELDS 1 otp
1) (integer) 1
127.0.0.1:6399> HTTL session:42 FIELDS 2 user otp
1) (integer) -1   # 'user' has no expiration
2) (integer) 5    # 'otp' is ticking down
```
Wait 5 seconds and run `HGETALL session:42`. `otp` is gone; `user` is still sitting right there.

#### Experiment 3: Compare-and-Set by Digest & Bounded Increment (Redis 8.4–8.8 `DIGEST` / `SET IFDEQ` / `INCREX`)
Why ship a 5MB value back over the network just to check if it changed before overwriting it? Compute its 64-bit XXH3 digest instead:

```text
127.0.0.1:6399> SET doc "v1-content"
OK
127.0.0.1:6399> DIGEST doc
"6f5b999a01b6f50b"
# Overwrite ONLY if the current digest still matches 6f5b999a01b6f50b:
127.0.0.1:6399> SET doc "v2-content" IFDEQ 6f5b999a01b6f50b
OK
# Add 10 to a counter, clamping at upper bound 100 (returns [new_val, actual_delta]):
127.0.0.1:6399> SET quota 95
OK
127.0.0.1:6399> INCREX quota BYINT 10 LBOUND 0 UBOUND 100 SATURATE
1) "100"
2) "5"
```

#### Experiment 4: Find Nearest Neighbors in an HNSW Graph Without Storing Full `float32`s (`VADD` / `VSIM`)
Strip away the "AI vector database" buzzwords, and what you're really doing is finding points with the smallest distance in a multi-dimensional graph. Godis builds an in-memory HNSW graph and lets you quantize vectors into 8-bit integers (`Q8`) or 1-bit binary (`BIN`), computing distances directly on the quantized codes:

```text
127.0.0.1:6399> VADD items VALUES 3 1.0 0.5 0.2 itemA Q8 SETATTR '{"year":2026,"tag":"go"}'
(integer) 1
127.0.0.1:6399> VADD items VALUES 3 0.9 0.4 0.3 itemB Q8 SETATTR '{"year":2024,"tag":"c"}'
(integer) 1
127.0.0.1:6399> VSIM items VALUES 3 1.0 0.5 0.2 COUNT 2 FILTER '.year >= 2025' WITHSCORES
1) "itemA"
2) "1"
```

#### Experiment 5: Reclaim Stalled Stream Jobs on the Fly & Block on Time Series (Redis 8.4–8.10)
Instead of running a separate `XAUTOCLAIM` loop when a worker dies, `XREADGROUP ... CLAIM` lets a healthy worker grab idle pending entries right before reading new ones—and `TS.READ BLOCK` lets you wait for new time-series samples just like `XREAD`:

```text
# Claim pending messages idle for >= 5000ms alongside new '>' messages:
127.0.0.1:6399> XREADGROUP GROUP g1 worker2 COUNT 10 CLAIM 5000 STREAMS orders >

# Block up to 5000ms until at least 1 new sample arrives on cpu:load:
127.0.0.1:6399> TS.READ BLOCK 5000 1 STREAMS cpu:load $
```

---

## Under the Hood: What Each Piece Actually Does

Forget the marketing labels. Here is what each subsystem actually does in plain English, and exactly where to find it in the repository:

| Subsystem | Stripped of Jargon, What Is It Doing? | Representative Commands | Where in the Code? |
| :--- | :--- | :--- | :--- |
| **Sharded Dict & Core Five Types** | Hashes keys via FNV-32 into shards, locks multiple keys in deterministic sorted order to prevent deadlocks, and stores Strings, Quicklists, Hashes, Sets, and Skiplist ZSets | `SET` (`IFEQ`/`IFDEQ`), `MSETEX`, `LMOVEM`/`BLMOVEM`, `SUNIONCARD`/`SDIFFCARD`, `ZUNION` (`AGGREGATE COUNT`) | [`datastruct/dict/`](datastruct/dict/)<br>[`database/`](database/) |
| **Dual-Shape RESP2 / RESP3** | Commands return a single reply object that serializes as classic RESP2 arrays (`*`) by default, or native RESP3 Maps (`%`), Sets (`~`), Doubles (`,`), and Pushes (`>`) after `HELLO 3` | `HELLO 3`, `CLIENT TRACKING`, `HGETALL`, `XREAD` | [`redis/protocol/`](redis/protocol/)<br>[`redis/parser/`](redis/parser/) |
| **Streams & Consumer Groups** | Stores ordered entries, tracks per-group Pending Entry Lists (PEL), supports blocking reads, producer idempotency deduplication, inline claiming, and negative acknowledgments | `XADD` (`IDMP`/`IDMPAUTO`), `XCFGSET`, `XREADGROUP` (`CLAIM`/`MAXCOUNT`/`MAXSIZE`), `XNACK`, `XDELEX` | [`datastruct/stream/`](datastruct/stream/)<br>[`database/stream*.go`](database/stream.go) |
| **JSON & JSONPath** | Parses documents into an in-memory tree and evaluates `[?(...)]` filters (comparisons, boolean logic, arithmetic, `in`/`nin`, `size`/`empty`, regex, and aggregate functions) | `JSON.SET`, `JSON.GET`, `JSON.NUMINCRBY`, `JSON.ARRAPPEND` | [`datastruct/json/`](datastruct/json/)<br>[`database/json*.go`](database/json.go) |
| **Vector Set** | Multi-layer HNSW graph in memory (or optional SQLite `sqlite-vec` backend). Supports `f32`, `Q8` (in-graph `int8` distance without dequantization buffers), and `BIN` (Hamming distance) | `VADD`, `VSIM`, `VREM`, `VLINKS`, `VRANGE`, `VSETATTR` (plus `VS.*` aliases) | [`datastruct/vector/`](datastruct/vector/)<br>[`database/vector*.go`](database/vector.go) |
| **RediSearch & Hybrid Search** | Inverted index + BM25 scoring + synonym expansion + cursor aggregation pipeline (or optional SQLite `FTS5` backend), plus RRF/Linear text + vector KNN fusion | `FT.CREATE`, `FT.SEARCH`, `FT.AGGREGATE` (`COLLECT`/`APPLY`), `FT.HYBRID`, `FT.ALIASLIST` | [`datastruct/redisearch/`](datastruct/redisearch/)<br>[`database/redisearch*.go`](database/redisearch.go) |
| **Time Series** | Timestamped float64 sequences (with `NaN` awareness), compaction/downsampling rules, time-weighted averages (`twa`), multi-aggregator queries, and blocking reads | `TS.CREATE`, `TS.ADD`, `TS.MRANGE`, `TS.NRANGE`, `TS.QUERYLABELS`, `TS.READ` (`BLOCK`) | [`datastruct/timeseries/`](datastruct/timeseries/)<br>[`database/timeseries*.go`](database/timeseries.go) |
| **Probabilistic & HLL** | Trades tiny, bounded error for massive memory savings: membership tests, frequency sketches, quantiles, and byte-for-byte Redis-compatible `HYLL` HyperLogLogs | `PFADD`/`PFCOUNT`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `TDIGEST.*` | [`datastruct/hll/`](datastruct/hll/)<br>[`database/probabilistic.go`](database/probabilistic.go) |
| **Lua & Redis Functions** | Sandboxed `gopher-lua` VM pool (`os`/`io` stripped) with built-in `cjson`, `cmsgpack`, and `bit` modules, plus persisted function libraries | `EVAL`, `EVAL_RO`, `FUNCTION LOAD`, `FCALL` | [`scripting/`](scripting/)<br>[`database/scripting.go`](database/scripting.go) |
| **Persistence, Replication & Cluster** | AOF, RDB, and RDB-preamble hybrid persistence; PSYNC2 ring-buffer replication and coordinated `FAILOVER`; 16,384 CRC16 hash slots + Hashicorp Raft topology consensus | `BGREWRITEAOF`, `REPLICAOF`, `FAILOVER`, `WAIT`/`WAITAOF`, `CLUSTER *` | [`aof/`](aof/)<br>[`cluster/`](cluster/)<br>[`database/replication*.go`](database/replication_master.go) |

See [`commands.md`](commands.md) for the full command catalog.

---

## Rule #1 Is Not Fooling Yourself: The Honest Boundaries

Most READMEs read like commercials—they brag about every command name they registered and quietly sweep the differences under the rug. In engineering, just like in science, **the first principle is that you must not fool yourself—and you are the easiest person to fool**.

Before you use or study Godis, here is the unvarnished ledger of what is real, what is approximated, and what is intentionally left out (documented in full detail in [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)):

1. **How We Measure Compatibility**
   - We don't guess. [`scripts/r4-1-cases.txt`](scripts/r4-1-cases.txt) runs **12,628 deterministic command cases** side-by-side against a real Redis 8.10 sidecar and diffs the wire responses and error strings.
   - Any raw `\r` or `\n` injected into error messages by user input is sanitized at the wire boundary (matching the Redis 8.10 security fix) so malformed input can never split a RESP frame.

2. **Where We Use Different Algorithms (and Tell You Up Front)**
   - **No Fake `jemalloc` Numbers**: This is a Go program. When you run `INFO memory` or `MEMORY STATS`, `mem_allocator` reports `go`. We report real OS process RSS (`used_memory_rss`) on Linux and Windows, and Go runtime heap counters, rather than inventing fake `jemalloc` arena numbers.
   - **`HOTKEYS` and `allkeys-lrm`**: Godis implements `HOTKEYS` via per-key command duration attribution and argument byte sampling rather than Redis's internal per-slot probabilistic structure; `volatile-lrm` / `allkeys-lrm` track modification times on command write keys.
   - **`SUNIONCARD ... APPROX`**: Godis accepts the `APPROX` keyword and returns the exact cardinality—which is always within 0% error of the true answer.

3. **Where We Refuse to Fake Interoperability**
   - **Module RDB Payloads**: Core types and HyperLogLog (`HYLL`) interoperate with Redis. Extended types (JSON, Vector, TimeSeries, Probabilistic structures, Array, RediSearch index definitions) persist via Godis's own `GODIS1` envelope. If you feed an official Redis module RDB payload into `RESTORE`, Godis explicitly rejects it with an `ERR` **before** touching existing state—never silently corrupting or dropping your data.
   - **`FUNCTION DUMP` is now officially interoperable (not a rejection case)**: Godis emits the Redis RDB-style `0xF5` + RDB string + version/CRC64 payload; `FUNCTION RESTORE` accepts the official format (including LZF-compressed code) and still accepts legacy `GODISFN1`. Verified bidirectionally with Redis 8.10 via DUMP→RESTORE→`FCALL`.

4. **What Is a Subset or Explicitly Out of Scope**
   - **Transactions (`MULTI`/`EXEC`)**: Aligned with standard Redis semantics—syntax errors during queueing abort the transaction (`EXECABORT`), while runtime errors on individual commands (like `LPUSH` on a string key) are returned as elements in the reply array without rolling back prior commands.
   - **Array Preview (Redis 8.8)**: All 18 preview commands (`ARSET`, `ARMSET`, `ARGET`, `ARMGET`, `ARGETRANGE`, `ARLEN`, `ARCOUNT`, `ARDEL`, `ARDELRANGE`, `ARINSERT`, `ARNEXT`, `ARSEEK`, `ARINFO`, `ARLASTITEMS`, `AROP`, `ARGREP`, `ARRING`, `ARSCAN`) and `TYPE array` are wired up. The internal model is a flat slot array; `ARINFO` slice statistics are synthesized and `FULL` returns only the basic fields.
   - **Explicit Non-Goals**: Compact hash internal template encoding (`HIMPORT` sessions work and write into standard Hashes), multi-part AOF `BACKUP`, full binary Redis gossip bus (Godis cluster uses Raft for topology), and the C Modules API.

Nothing is hidden. Knowing the exact boundaries is what makes a system trustworthy.

---

## How to Tour the Codebase

If you want to read the code, don't start by memorizing a 400-entry command table. Follow **the life of a single command** from the socket to the shard and back—you'll have the whole architecture mapped out in half an hour:

```text
godis/
├── main.go                     # 1. Entry point: flags, config loading, signal handling, server startup
├── tcp/ & redis/server/        # 2. Networking: std (one goroutine per conn) or gnet (event-driven)
├── redis/parser/ & protocol/   # 3. Wire framing: parses RESP2/RESP3 streams and encodes replies
├── database/
│   ├── server.go               # 4. Top dispatcher: AUTH, ACL, CLIENT PAUSE, slowlog, multi-DB routing
│   ├── router.go               # 5. The phonebook: registers 440+ commands, key specs, and lock pre-funcs
│   ├── database.go             # 6. Single-DB engine: RWLocks -> executor -> AOF -> client cache hooks
│   ├── string.go / list.go / hash.go / set.go / sortedset.go / stream*.go
│   ├── json*.go / vector*.go / timeseries*.go / redisearch*.go / array.go
│   ├── probabilistic.go / tdigest.go / hyperloglog.go / hotkeys_himport.go
│   └── replication_*.go / failover.go / caching.go / acl.go / transaction.go
├── datastruct/                 # 7. Data structures: sharded dict, skiplist, quicklist, HNSW graph, inverted index
├── aof/                        # 8. Persistence: AOF writer, background rewrite, RDB & GODIS1 codecs
├── scripting/                  # 9. Scripting: gopher-lua VM pool and persisted Redis Functions
└── cluster/                    # 10. Cluster: 16,384-slot routing, MOVED/ASK, TCC, and Raft FSM
```

Want to switch the search and vector engines from the default in-memory implementation to the SQLite backend (`FTS5` + `sqlite-vec`)? Build with the `sqlite_backend` tag:

```bash
go build -tags sqlite_backend -o godis ./
```

For deeper dives, check out:
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Request lifecycle, locking model, and component boundaries
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — Graceful shutdown, `/health`, Prometheus `/metrics`, and ACL persistence
- [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) — Detailed Redis 8.10 compatibility matrix

---

## How Fast Is It?

Here is a `redis-benchmark` run on `Go 1.23` (`macOS Monterey, Apple M2 Air`):

```text
PING_INLINE:    179,211.45 requests per second, p50=1.031 msec
PING_MBULK:     173,611.12 requests per second, p50=1.071 msec
SET:            158,478.61 requests per second, p50=1.535 msec
GET:            156,985.86 requests per second, p50=1.127 msec
INCR:           164,473.69 requests per second, p50=1.063 msec
LPUSH:          151,285.92 requests per second, p50=1.079 msec
RPUSH:          176,678.45 requests per second, p50=1.023 msec
LPOP:           177,619.89 requests per second, p50=1.039 msec
SADD:           159,489.64 requests per second, p50=1.047 msec
HSET:           175,131.36 requests per second, p50=1.031 msec
ZADD:           165,289.25 requests per second, p50=1.039 msec
ZPOPMIN:        185,528.77 requests per second, p50=0.999 msec
LRANGE_100:      46,511.62 requests per second, p50=4.063 msec
MSET (10 keys):  88,417.33 requests per second, p50=3.687 msec
```

A word of caution: never blindly trust benchmark numbers printed in someone else's README. OS schedulers, pipeline depth, `appendfsync` policy, and whether you run `std` or `use-gnet yes` will shift the numbers dramatically. The only benchmark that matters is the one you run on your own hardware with `redis-benchmark -p 6399`.

---

## Contributing

Found a bug where Godis disagrees with Redis? Want to tighten a data structure or add a missing edge case? Jump right in!

Before opening a PR:
1. Run `go test ./...` and `go vet ./...` to make sure nothing broke.
2. Add a test that proves your fix works (and if the command is deterministic, add a case to [`scripts/r4-1-cases.txt`](scripts/r4-1-cases.txt)).
3. If a feature is a subset or uses an approximation, document it honestly in [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md).

## Related Projects & License

- [godis-dashboard](https://github.com/linkerlin/godis-dashboard): Web UI dashboard for Godis
- [Design Blog Series (Chinese)](https://www.cnblogs.com/Finley/category/1598973.html): Notes on building Godis from the ground up
- Licensed under the [MIT License](./LICENSE). Take it apart, tinker with it, and have fun!
