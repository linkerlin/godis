package database

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// parseXReadBlockTimeout parses XREAD/XREADGROUP BLOCK milliseconds (integer).
// Aligns Redis: negative vs non-integer messages.
func parseXReadBlockTimeout(arg []byte) (int64, redis.Reply) {
	ms, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return 0, protocol.MakeErrReply("ERR timeout is not an integer or out of range")
	}
	if ms < 0 {
		return 0, protocol.MakeErrReply("ERR timeout is negative")
	}
	return ms, nil
}

// bulkWireLen returns the RESP wire length of a bulk string of n payload bytes.
func bulkWireLen(n int64) int64 {
	digits := 1
	for v := n; v >= 10; v /= 10 {
		digits++
	}
	return 1 + int64(digits) + 2 + n + 2
}

// xreadEntryWireBytes estimates the RESP wire size of one entry as emitted in
// an XREAD/XREADGROUP reply (ID bulk + field/value bulks, framing included).
// Used by the MAXSIZE cumulative budget (Redis 8.10).
func xreadEntryWireBytes(e *stream.StreamEntry) int64 {
	sz := bulkWireLen(int64(len(e.ID.String())))
	for k, v := range e.Fields {
		sz += bulkWireLen(int64(len(k))) + bulkWireLen(int64(len(v)))
	}
	return sz
}

// xreadReplyCaps tracks the cumulative MAXCOUNT/MAXSIZE budgets of one
// XREAD/XREADGROUP invocation (0 = unlimited).
type xreadReplyCaps struct {
	maxCount int64
	maxSize  int64
	total    int64
	bytes    int64
}

// capEntries truncates entries (already capped per-stream by COUNT) to the
// cumulative budgets. At least one entry is always returned overall.
func (c *xreadReplyCaps) capEntries(entries []*stream.StreamEntry) []*stream.StreamEntry {
	if c.maxCount <= 0 && c.maxSize <= 0 {
		return entries
	}
	out := entries[:0]
	for _, e := range entries {
		if c.maxCount > 0 && c.total >= c.maxCount {
			break
		}
		sz := xreadEntryWireBytes(e)
		if c.maxSize > 0 && c.total > 0 && c.bytes+sz > c.maxSize {
			break
		}
		out = append(out, e)
		c.total++
		c.bytes += sz
	}
	return out
}

// remaining returns the per-stream entry budget (0 = unlimited).
func (c *xreadReplyCaps) remaining() int64 {
	if c.maxCount <= 0 {
		return 0
	}
	return c.maxCount - c.total
}

// execXRead 从Stream读取数据
// XREAD [COUNT count] [MAXCOUNT maxcount] [MAXSIZE maxsize] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
func execXRead(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xread' command")
	}

	count := -1
	maxCount := int64(0)
	maxSize := int64(0)
	blockTimeout := time.Duration(-1) // -1 表示不阻塞

	// 解析选项
	i := 0
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			c, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			// Redis: COUNT ≤ 0 means unlimited.
			if c > 0 {
				count = c
			}
			i += 2
		case "MAXCOUNT", "MAXSIZE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if arg == "MAXCOUNT" {
				maxCount = n
			} else {
				maxSize = n
			}
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, errReply := parseXReadBlockTimeout(args[i+1])
			if errReply != nil {
				return errReply
			}
			blockTimeout = time.Duration(ms) * time.Millisecond
			i += 2
		case "STREAMS":
			i++
			goto parseStreams
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

parseStreams:
	// 解析stream keys和ids
	remaining := len(args) - i
	if remaining < 2 || remaining%2 != 0 {
		return protocol.MakeErrReply("ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified")
	}

	numStreams := remaining / 2
	keys := make([]string, numStreams)
	ids := make([]string, numStreams)

	for j := 0; j < numStreams; j++ {
		keys[j] = string(args[i+j])
		ids[j] = string(args[i+numStreams+j])
	}
	if reply := validateStreamKeyNames(keys); reply != nil {
		return reply
	}

	// Resolve "$" once at command start (Redis BLOCK+$ semantics).
	resolvedIDs := make([]stream.StreamID, numStreams)
	for j, key := range keys {
		if ids[j] == "$" {
			s, errReply := db.getAsStream(key)
			if errReply != nil {
				return errReply
			}
			if s != nil {
				resolvedIDs[j] = s.GetLastID()
			}
		} else {
			id, err := stream.ParseStreamID(ids[j], stream.StreamID{})
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
			resolvedIDs[j] = id
		}
	}

	if maxCount > 0 && count > 0 && maxCount < int64(count) {
		return protocol.MakeErrReply("ERR MAXCOUNT can't be less than COUNT")
	}

	startTime := time.Now()
	caps := &xreadReplyCaps{maxCount: maxCount, maxSize: maxSize}
	for {
		var buckets []streamReadBucket
		caps.total, caps.bytes = 0, 0

		for j, key := range keys {
			s, errReply := db.getAsStream(key)
			if errReply != nil {
				return errReply
			}
			if s == nil {
				continue
			}

			startID := resolvedIDs[j]
			entries := s.Range(startID, stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1})

			var filtered []*stream.StreamEntry
			for _, entry := range entries {
				if entry.ID.Compare(startID) > 0 {
					filtered = append(filtered, entry)
				}
			}

			// Per-stream COUNT cap plus the cumulative MAXCOUNT remainder.
			perStream := int64(count) // -1 = unlimited
			if caps.maxCount > 0 {
				rem := caps.remaining()
				if rem <= 0 {
					break
				}
				if perStream <= 0 || perStream > rem {
					perStream = rem
				}
			}
			if perStream >= 0 && int64(len(filtered)) > perStream {
				filtered = filtered[:perStream]
			}
			filtered = caps.capEntries(filtered)

			if len(filtered) > 0 {
				buckets = append(buckets, streamReadBucket{key: key, entries: filtered})
			}
		}

		if len(buckets) > 0 {
			return MakeStreamReadReply(buckets)
		}

		if blockTimeout < 0 {
			return &protocol.NullBulkReply{}
		}
		waitDur := blockTimeout
		if blockTimeout > 0 {
			elapsed := time.Since(startTime)
			if elapsed >= blockTimeout {
				return &protocol.NullBulkReply{}
			}
			waitDur = blockTimeout - elapsed
		}
		w := registerStreamWaiter(keys)
		signaled := waitOrTimeout(w.ch, waitDur)
		unregisterStreamWaiter(w)
		if r := replyAfterWait(w, signaled); r != nil {
			return r
		}
	}
}

// execXReadGroup 从消费者组读取数据
// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
func execXReadGroup(db *DB, args [][]byte) redis.Reply {
	if len(args) < 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xreadgroup' command")
	}

	// 解析GROUP
	if strings.ToUpper(string(args[0])) != "GROUP" {
		return protocol.MakeSyntaxErrReply()
	}
	groupName := string(args[1])
	consumerName := string(args[2])

	count := -1
	maxCount := int64(0)
	maxSize := int64(0)
	blockTimeout := time.Duration(-1)
	noAck := false
	claimMinIdle := int64(-1) // XREADGROUP CLAIM min-idle-time ms (8.4); -1 = absent

	// 解析选项
	i := 3
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			c, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			// Redis: COUNT ≤ 0 means unlimited.
			if c > 0 {
				count = c
			}
			i += 2
		case "CLAIM":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			claimMinIdle = n
			i += 2
		case "MAXCOUNT", "MAXSIZE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if arg == "MAXCOUNT" {
				maxCount = n
			} else {
				maxSize = n
			}
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, errReply := parseXReadBlockTimeout(args[i+1])
			if errReply != nil {
				return errReply
			}
			blockTimeout = time.Duration(ms) * time.Millisecond
			i += 2
		case "NOACK":
			noAck = true
			i++
		case "STREAMS":
			i++
			goto parseStreams
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

parseStreams:
	// 解析stream keys和ids
	remaining := len(args) - i
	if remaining < 2 || remaining%2 != 0 {
		return protocol.MakeErrReply("ERR Unbalanced XREADGROUP list of streams")
	}

	numStreams := remaining / 2
	keys := make([]string, numStreams)
	ids := make([]string, numStreams)

	for j := 0; j < numStreams; j++ {
		keys[j] = string(args[i+j])
		ids[j] = string(args[i+numStreams+j])
	}
	if reply := validateStreamKeyNames(keys); reply != nil {
		return reply
	}

	if maxCount > 0 && count > 0 && maxCount < int64(count) {
		return protocol.MakeErrReply("ERR MAXCOUNT can't be less than COUNT")
	}

	startTime := time.Now()
	caps := &xreadReplyCaps{maxCount: maxCount, maxSize: maxSize}
	for {
		var buckets []streamReadBucket
		caps.total, caps.bytes = 0, 0

		for j, key := range keys {
			s, errReply := db.getAsStream(key)
			if errReply != nil {
				return errReply
			}
			if s == nil {
				continue
			}

			// 获取消费者组
			group, err := s.GetGroup(groupName)
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}

			group.Lock()

			// 获取消费者
			consumer := group.GetConsumerLocked(consumerName)
			consumer.SeenTime = time.Now()

			var entries []*stream.StreamEntry
			id := ids[j]
			// XREADGROUP CLAIM (8.4): claimed pending entries form a prefix
			// of this bucket, delivered before new entries.
			claimPEs := []*stream.PendingEntry(nil)
			claimIdle := []int64(nil)

			if id == ">" {
				if claimMinIdle >= 0 {
					now := time.Now()
					type claimCand struct {
						id    stream.StreamID
						entry *stream.StreamEntry
						pe    *stream.PendingEntry
						idle  int64
					}
					var cands []claimCand
					for pid, pe := range group.Pending {
						idle := int64(math.MaxInt64)
						if !pe.DeliveryTime.IsZero() {
							idle = now.Sub(pe.DeliveryTime).Milliseconds()
							if idle < 0 {
								idle = 0
							}
						}
						if idle < claimMinIdle {
							continue
						}
						entry := s.GetEntry(pid)
						if entry == nil {
							continue // trimmed payload — approximation: skipped
						}
						cands = append(cands, claimCand{id: pid, entry: entry, pe: pe, idle: idle})
					}
					sort.Slice(cands, func(a, b int) bool {
						za, zb := cands[a].pe.DeliveryTime.IsZero(), cands[b].pe.DeliveryTime.IsZero()
						if za != zb {
							return za // XNACK-released entries claim first
						}
						if za {
							return cands[a].id.Compare(cands[b].id) < 0
						}
						if cands[a].idle != cands[b].idle {
							return cands[a].idle > cands[b].idle // longer idle first
						}
						return cands[a].id.Compare(cands[b].id) < 0
					})
					for _, c := range cands {
						entries = append(entries, c.entry)
						claimPEs = append(claimPEs, c.pe)
						if c.pe.DeliveryTime.IsZero() {
							claimIdle = append(claimIdle, -1)
						} else {
							claimIdle = append(claimIdle, c.idle)
						}
					}
				}

				// 读取新消息（从未递送过的）
				lastID := group.LastID
				allEntries := s.Range(lastID, stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1})

				// 过滤已递送的
				for _, entry := range allEntries {
					if entry.ID.Compare(lastID) > 0 {
						// 检查是否已经在组pending中
						if _, exists := group.Pending[entry.ID]; !exists {
							entries = append(entries, entry)
						}
					}
				}
				// group.LastID 在截断后按实际递送的新条目更新（见下方 caps 处）
			} else if id == "$" {
				// 只读取新数据
				lastID := s.GetLastID()
				allEntries := s.Range(lastID, stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1})
				for _, entry := range allEntries {
					if entry.ID.Compare(lastID) > 0 {
						entries = append(entries, entry)
					}
				}
			} else {
				// 从历史 pending 中读取（重新读取已递送但未确认的消息）
				startID, err := stream.ParseStreamID(id, stream.StreamID{})
				if err != nil {
					group.Unlock()
					return protocol.MakeErrReply("ERR Invalid stream ID")
				}

				type pendItem struct {
					id    stream.StreamID
					entry *stream.StreamEntry
				}
				var items []pendItem
				for pid := range consumer.Pending {
					if pid.Compare(startID) < 0 {
						continue
					}
					entry := s.GetEntry(pid)
					if entry == nil {
						continue
					}
					items = append(items, pendItem{id: pid, entry: entry})
				}
				sort.Slice(items, func(i, j int) bool {
					return items[i].id.Compare(items[j].id) < 0
				})
				for _, it := range items {
					entries = append(entries, it.entry)
				}
			}

			// Apply the per-stream COUNT cap plus the cumulative
			// MAXCOUNT/MAXSIZE budgets. Claimed entries are the prefix, so
			// entries dropped by the caps are never claimed (their PEL state
			// is untouched).
			perStream := int64(count) // -1 = unlimited
			if caps.maxCount > 0 {
				rem := caps.remaining()
				if rem <= 0 {
					group.Unlock()
					break
				}
				if perStream <= 0 || perStream > rem {
					perStream = rem
				}
			}
			if perStream >= 0 && int64(len(entries)) > perStream {
				entries = entries[:perStream]
			}
			entries = caps.capEntries(entries)
			keptClaimed := 0
			if len(claimPEs) > 0 {
				keptClaimed = len(entries)
				if keptClaimed > len(claimPEs) {
					keptClaimed = len(claimPEs)
				}
			}
			// Advance the group's LastID only over NEW entries (claimed
			// entries were delivered before and are ≤ LastID).
			if id == ">" && len(entries) > keptClaimed {
				group.LastID = entries[len(entries)-1].ID
			}

			if len(entries) > 0 {
				// Claim the surviving pending prefix to this consumer
				// (NOACK does not apply to claimed entries).
				now := time.Now()
				for k := 0; k < keptClaimed; k++ {
					pe := claimPEs[k]
					if pe.Consumer != "" && pe.Consumer != consumerName {
						if raw, exists := group.Consumers.Get(pe.Consumer); exists {
							delete(raw.(*stream.Consumer).Pending, pe.ID)
						}
					}
					pe.Consumer = consumerName
					pe.DeliveryTime = now
					pe.DeliveryCount++
					consumer.Pending[pe.ID] = pe
					group.Pending[pe.ID] = pe
				}
				// 添加到pending（除非NOACK）；历史重读递增 DeliveryCount
				if !noAck {
					historyReread := id != ">" && id != "$"
					for _, entry := range entries[keptClaimed:] {
						if historyReread {
							if pe, ok := consumer.Pending[entry.ID]; ok {
								pe.DeliveryCount++
								pe.DeliveryTime = now
								group.Pending[entry.ID] = pe
								continue
							}
						}
						consumer.Pending[entry.ID] = &stream.PendingEntry{
							ID:            entry.ID,
							Consumer:      consumerName,
							DeliveryTime:  now,
							DeliveryCount: 1,
						}
						group.Pending[entry.ID] = consumer.Pending[entry.ID]
					}
				}

				bucket := streamReadBucket{key: key, entries: entries}
				if keptClaimed > 0 {
					bucket.claimedCount = keptClaimed
					bucket.claimIdle = claimIdle[:keptClaimed]
					bucket.claimDeliver = make([]int, keptClaimed)
					for k := 0; k < keptClaimed; k++ {
						bucket.claimDeliver[k] = claimPEs[k].DeliveryCount
					}
				}
				buckets = append(buckets, bucket)
			}
			group.Unlock()
		}

		if len(buckets) > 0 {
			return MakeStreamReadReply(buckets)
		}

		if blockTimeout < 0 {
			return &protocol.NullBulkReply{}
		}
		// History pending reads (id != ">") do not wait for XADD.
		onlyHistory := true
		for _, id := range ids {
			if id == ">" || id == "$" {
				onlyHistory = false
				break
			}
		}
		if onlyHistory {
			return &protocol.NullBulkReply{}
		}
		waitDur := blockTimeout
		if blockTimeout > 0 {
			elapsed := time.Since(startTime)
			if elapsed >= blockTimeout {
				return &protocol.NullBulkReply{}
			}
			waitDur = blockTimeout - elapsed
		}
		w := registerStreamWaiter(keys)
		signaled := waitOrTimeout(w.ch, waitDur)
		unregisterStreamWaiter(w)
		if r := replyAfterWait(w, signaled); r != nil {
			return r
		}
	}
}

// execXClaim claims pending messages for a consumer
// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time]
//
//	[RETRYCOUNT count] [FORCE] [JUSTID]
func execXClaim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xclaim' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	consumerName := string(args[2])
	minIdleMs, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil || minIdleMs < 0 {
		return protocol.MakeErrReply("ERR Invalid min-idle-time argument for XCLAIM")
	}

	opts := &stream.ClaimOptions{}
	ids := make([]stream.StreamID, 0)
	i := 4
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "IDLE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR Unrecognized XCLAIM option 'IDLE'")
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid IDLE option argument for XCLAIM")
			}
			opts.Idle = time.Duration(ms) * time.Millisecond
			i += 2
		case "TIME":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR Unrecognized XCLAIM option 'TIME'")
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid TIME option argument for XCLAIM")
			}
			opts.Time = time.UnixMilli(ms)
			i += 2
		case "RETRYCOUNT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR Unrecognized XCLAIM option 'RETRYCOUNT'")
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR Invalid RETRYCOUNT option argument for XCLAIM")
			}
			opts.RetryCount = n
			i += 2
		case "FORCE":
			opts.Force = true
			i++
		case "JUSTID":
			opts.JustID = true
			i++
		case "LASTID":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR Unrecognized XCLAIM option 'LASTID'")
			}
			if _, err := stream.ParseStreamID(string(args[i+1]), stream.StreamID{}); err != nil {
				return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
			}
			i += 2 // accepted but unused
		default:
			id, err := stream.ParseStreamID(string(args[i]), stream.StreamID{})
			if err != nil {
				return protocol.MakeErrReply("ERR Unrecognized XCLAIM option '" + string(args[i]) + "'")
			}
			ids = append(ids, id)
			i++
		}
	}
	if len(ids) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xclaim' command")
	}

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}
	if _, err := s.GetGroup(groupName); err != nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}

	claimed, err := s.Claim(groupName, consumerName, time.Duration(minIdleMs)*time.Millisecond, ids, opts)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	db.addAof(utils.ToCmdLine3("xclaim", args...))

	if opts.JustID {
		idBytes := make([][]byte, len(claimed))
		for i, e := range claimed {
			idBytes[i] = []byte(e.ID.String())
		}
		return protocol.MakeMultiBulkReply(idBytes)
	}
	return streamEntriesToReply(claimed)
}

// execXAutoClaim claims idle pending messages starting from a cursor
// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
func execXAutoClaim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xautoclaim' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	consumerName := string(args[2])
	minIdleMs, err := strconv.ParseInt(string(args[3]), 10, 64)
	if err != nil || minIdleMs < 0 {
		return protocol.MakeErrReply("ERR Invalid min-idle-time argument for XAUTOCLAIM")
	}
	start, err := stream.ParseStreamID(string(args[4]), stream.StreamID{})
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
	}
	count := 100
	justID := false
	var minID *stream.StreamID
	for i := 5; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			c, err := strconv.Atoi(string(args[i+1]))
			if err != nil || c <= 0 {
				return protocol.MakeErrReply("ERR COUNT must be > 0")
			}
			count = c
			i += 2
		case "JUSTID":
			justID = true
			i++
		case "MINID":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			id, err := stream.ParseStreamID(string(args[i+1]), stream.StreamID{})
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
			}
			minID = &id
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}
	if _, err := s.GetGroup(groupName); err != nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}

	claimed, deleted, nextID, err := s.AutoClaim(groupName, consumerName,
		time.Duration(minIdleMs)*time.Millisecond, start, count, minID)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	db.addAof(utils.ToCmdLine3("xautoclaim", args...))

	var entriesReply redis.Reply
	if justID {
		ids := make([][]byte, len(claimed))
		for i, e := range claimed {
			ids[i] = []byte(e.ID.String())
		}
		entriesReply = protocol.MakeMultiBulkReply(ids)
	} else {
		entriesReply = streamEntriesToReply(claimed)
	}
	deletedBytes := make([][]byte, len(deleted))
	for i, id := range deleted {
		deletedBytes[i] = []byte(id.String())
	}
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte(nextID.String())),
		entriesReply,
		protocol.MakeMultiBulkReply(deletedBytes),
	})
}

// execXAck 确认消息已处理
// XACK key group id [id ...]
func execXAck(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xack' command")
	}

	key := string(args[0])
	groupName := string(args[1])

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeIntReply(0)
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeIntReply(0)
	}

	// 解析IDs
	ids := make([]stream.StreamID, len(args)-2)
	for i := 2; i < len(args); i++ {
		id, err := stream.ParseStreamID(string(args[i]), stream.StreamID{})
		if err != nil {
			return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
		}
		ids[i-2] = id
	}

	group.Lock()
	acked := 0
	for _, id := range ids {
		// 从组的pending中删除
		if pending, exists := group.Pending[id]; exists {
			delete(group.Pending, id)

			// 从消费者的pending中删除
			if consumer, ok := group.Consumers.Get(pending.Consumer); ok {
				delete(consumer.(*stream.Consumer).Pending, id)
			}

			acked++
		}
	}
	group.Unlock()

	if acked > 0 {
		db.addAof(utils.ToCmdLine3("xack", args...))
	}

	return protocol.MakeIntReply(int64(acked))
}

// execXPending 查看待处理消息
// XPENDING key group [[start end count] [consumer]]
func execXPending(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xpending' command")
	}

	key := string(args[0])
	groupName := string(args[1])

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}

	group.RLock()
	defer group.RUnlock()

	// 简单模式：只返回统计信息
	if len(args) == 2 {
		// 计算pending数量、最小ID、最大ID
		count := len(group.Pending)
		if count == 0 {
			// Redis: [0, null, null, null] — integer + three null bulks
			return protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeIntReply(0),
				&protocol.NullBulkReply{},
				&protocol.NullBulkReply{},
				&protocol.NullBulkReply{},
			})
		}

		var minID, maxID stream.StreamID
		consumers := make(map[string]int)

		first := true
		for id, pending := range group.Pending {
			if first || id.Compare(minID) < 0 {
				minID = id
			}
			if first || id.Compare(maxID) > 0 {
				maxID = id
			}
			consumers[pending.Consumer]++
			first = false
		}

		// 构建消费者列表
		consumerReplies := make([]redis.Reply, 0, len(consumers)*2)
		for name, c := range consumers {
			consumerReplies = append(consumerReplies,
				protocol.MakeBulkReply([]byte(name)),
				protocol.MakeIntReply(int64(c)))
		}

		return protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(int64(count)),
			protocol.MakeBulkReply([]byte(minID.String())),
			protocol.MakeBulkReply([]byte(maxID.String())),
			protocol.MakeMultiRawReply(consumerReplies),
		})
	}

	// 详细模式：XPENDING key group [IDLE min-idle-time | TIME start-time] start end count [consumer]
	if len(args) < 5 {
		return protocol.MakeSyntaxErrReply()
	}

	rest := args[2:]
	minIdleMs := int64(-1)
	minDeliveryUnixMs := int64(-1)
	idx := 0
	if len(rest) >= 2 {
		opt := strings.ToUpper(string(rest[0]))
		switch opt {
		case "IDLE":
			idle, err := strconv.ParseInt(string(rest[1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			// Redis accepts negative IDLE (no effective filter; idleMs is always ≥ 0).
			minIdleMs = idle
			idx = 2
		case "TIME":
			ts, err := strconv.ParseInt(string(rest[1]), 10, 64)
			if err != nil || ts < 0 {
				return protocol.MakeErrReply("ERR Invalid start-time")
			}
			minDeliveryUnixMs = ts
			idx = 2
		}
	}
	if len(rest)-idx < 3 {
		return protocol.MakeSyntaxErrReply()
	}

	startID, err := parseStreamRangeBound(string(rest[idx]))
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid stream ID")
	}

	endID, err := parseStreamRangeBound(string(rest[idx+1]))
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid stream ID")
	}

	count, err := strconv.Atoi(string(rest[idx+2]))
	if err != nil || count < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	var consumerFilter string
	if len(rest) > idx+3 {
		consumerFilter = string(rest[idx+3])
	}

	now := time.Now()
	type pendRow struct {
		id      stream.StreamID
		pending *stream.PendingEntry
	}
	var rows []pendRow
	for id, pending := range group.Pending {
		if id.Compare(startID) < 0 || id.Compare(endID) > 0 {
			continue
		}
		if consumerFilter != "" && pending.Consumer != consumerFilter {
			continue
		}
		// XNACK-released entries (delivery time zero) report idle -1,
		// matching Redis 8.8 XPENDING.
		idleMs := int64(-1)
		if !pending.DeliveryTime.IsZero() {
			idleMs = now.Sub(pending.DeliveryTime).Milliseconds()
		}
		if idleMs >= 0 && minIdleMs >= 0 && idleMs < minIdleMs {
			continue
		}
		if minDeliveryUnixMs >= 0 && pending.DeliveryTime.UnixMilli() < minDeliveryUnixMs {
			continue
		}
		rows = append(rows, pendRow{id: id, pending: pending})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].id.Compare(rows[j].id) < 0
	})

	replies := make([]redis.Reply, 0, len(rows))
	for i, row := range rows {
		if i >= count {
			break
		}
		idleMs := int64(-1)
		if !row.pending.DeliveryTime.IsZero() {
			idleMs = now.Sub(row.pending.DeliveryTime).Milliseconds()
		}
		replies = append(replies, protocol.MakeMultiBulkReply([][]byte{
			[]byte(row.id.String()),
			[]byte(row.pending.Consumer),
			[]byte(strconv.FormatInt(idleMs, 10)),
			[]byte(strconv.Itoa(row.pending.DeliveryCount)),
		}))
	}

	return protocol.MakeMultiRawReply(replies)
}

// parseStreamRangeBound accepts Redis exclusive range markers "-" / "+" or full stream IDs.
func parseStreamRangeBound(s string) (stream.StreamID, error) {
	switch s {
	case "-":
		return stream.StreamID{}, nil
	case "+":
		return stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1}, nil
	default:
		return stream.ParseStreamID(s, stream.StreamID{})
	}
}

// execXGroupCreateConsumer 创建消费者
// XGROUP CREATECONSUMER key group consumer
func execXGroupCreateConsumer(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup' command")
	}

	key := string(args[0])
	groupName := string(args[1])
	consumerName := string(args[2])

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		// Align with Redis 8.x XGROUP CREATECONSUMER on missing key.
		return protocol.MakeErrReply("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
	}

	// 检查消费者是否已存在
	if _, exists := group.Consumers.Get(consumerName); exists {
		return protocol.MakeIntReply(0)
	}

	// 创建消费者
	group.GetConsumer(consumerName)

	return protocol.MakeIntReply(1)
}

// execXGroupDelConsumer 删除消费者
// XGROUP DELCONSUMER key group consumer
func execXGroupDelConsumer(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup' command")
	}

	key := string(args[0])
	groupName := string(args[1])
	consumerName := string(args[2])

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
	}

	pendingCount, err := group.DeleteConsumer(consumerName)
	if err != nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(int64(pendingCount))
}

// execXInfoGroups 获取消费者组信息
// XINFO GROUPS key
func execXInfoGroups(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xinfo|groups' command")
	}

	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR no such key")
	}

	groups := s.GetGroups()
	replies := make([]redis.Reply, 0, len(groups))
	for _, group := range groups {
		group.RLock()
		// Each group is a Map: RESP3 → %, RESP2 → flat field/value array.
		gm := protocol.MakeMapReply()
		gm.Put("name", protocol.MakeBulkReply([]byte(group.Name)))
		gm.Put("consumers", protocol.MakeIntReply(int64(group.Consumers.Len())))
		gm.Put("pending", protocol.MakeIntReply(int64(len(group.Pending))))
		gm.Put("last-delivered-id", protocol.MakeBulkReply([]byte(group.LastID.String())))
		if group.EntriesRead >= 0 {
			gm.Put("entries-read", protocol.MakeIntReply(group.EntriesRead))
		}
		group.RUnlock()
		replies = append(replies, gm)
	}
	return protocol.MakeMultiRawReply(replies)
}

// execXInfoConsumers XINFO CONSUMERS key groupname
func execXInfoConsumers(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xinfo|consumers' command")
	}
	key := string(args[0])
	groupName := string(args[1])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR no such key")
	}
	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
	}

	group.RLock()
	defer group.RUnlock()

	var replies []redis.Reply
	group.Consumers.ForEach(func(name string, val interface{}) bool {
		c := val.(*stream.Consumer)
		idle := time.Since(c.SeenTime).Milliseconds()
		if c.SeenTime.IsZero() {
			idle = 0
		}
		// Each consumer is a Map: RESP3 → %, RESP2 → flat field/value array.
		cm := protocol.MakeMapReply()
		cm.Put("name", protocol.MakeBulkReply([]byte(c.Name)))
		cm.Put("pending", protocol.MakeIntReply(int64(len(c.Pending))))
		cm.Put("idle", protocol.MakeIntReply(idle))
		replies = append(replies, cm)
		return true
	})
	return protocol.MakeMultiRawReply(replies)
}

func init() {
	registerCommand("XRead", execXRead, noPrepare, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagBlocking}, 1, 1, 1)
	registerCommand("XReadGroup", execXReadGroup, noPrepare, nil, -6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagBlocking}, 1, 1, 1)
	registerCommand("XClaim", execXClaim, writeFirstKey, nil, -6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XAutoClaim", execXAutoClaim, writeFirstKey, nil, -6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XAck", execXAck, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XPending", execXPending, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	// Legacy concatenated names kept for backward compatibility
	registerCommand("XGroupCreateConsumer", execXGroupCreateConsumer, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XGroupDelConsumer", execXGroupDelConsumer, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XInfoGroups", execXInfoGroups, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
