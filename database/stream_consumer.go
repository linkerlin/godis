package database

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execXRead 从Stream读取数据
// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
func execXRead(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xread' command")
	}

	count := -1
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
			if err != nil || c <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
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

	startTime := time.Now()
	for {
		var buckets []streamReadBucket

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

			if count > 0 && len(filtered) > count {
				filtered = filtered[:count]
			}

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
	blockTimeout := time.Duration(-1)
	noAck := false

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
			if err != nil || c <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			count = c
			i += 2
		case "BLOCK":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
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

	startTime := time.Now()
	for {
		var buckets []streamReadBucket

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

			// 获取消费者
			consumer := group.GetConsumer(consumerName)
			consumer.SeenTime = time.Now()

			var entries []*stream.StreamEntry
			id := ids[j]

			if id == ">" {
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

				// 更新组的LastID
				if len(entries) > 0 {
					group.LastID = entries[len(entries)-1].ID
				}
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

			// 应用count限制
			if count > 0 && len(entries) > count {
				entries = entries[:count]
			}

			if len(entries) > 0 {
				// 添加到pending（除非NOACK）；历史重读递增 DeliveryCount
				if !noAck {
					now := time.Now()
					historyReread := id != ">" && id != "$"
					for _, entry := range entries {
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

				buckets = append(buckets, streamReadBucket{key: key, entries: entries})
			}
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
		return protocol.MakeErrReply("ERR Invalid min-idle-time")
	}

	opts := &stream.ClaimOptions{}
	ids := make([]stream.StreamID, 0)
	i := 4
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "IDLE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid IDLE")
			}
			opts.Idle = time.Duration(ms) * time.Millisecond
			i += 2
		case "TIME":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR Invalid TIME")
			}
			opts.Time = time.UnixMilli(ms)
			i += 2
		case "RETRYCOUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR count must be a non-negative integer")
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
				return protocol.MakeSyntaxErrReply()
			}
			i += 2 // accepted but unused
		default:
			id, err := stream.ParseStreamID(string(args[i]), stream.StreamID{})
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
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
		return protocol.MakeErrReply("ERR no such key")
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
		return protocol.MakeErrReply("ERR Invalid min-idle-time")
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
				return protocol.MakeErrReply("ERR COUNT must be a positive integer")
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
		return protocol.MakeErrReply("ERR no such key")
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
			return protocol.MakeErrReply("ERR Invalid stream ID")
		}
		ids[i-2] = id
	}

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
		return protocol.MakeEmptyMultiBulkReply()
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// 简单模式：只返回统计信息
	if len(args) == 2 {
		// 计算pending数量、最小ID、最大ID
		count := len(group.Pending)
		if count == 0 {
			nullReply := &protocol.NullBulkReply{}
			return protocol.MakeMultiBulkReply([][]byte{
				[]byte("0"),
				nullReply.ToBytes(),
				nullReply.ToBytes(),
				nullReply.ToBytes(),
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
		var consumerList [][]byte
		for name, c := range consumers {
			consumerList = append(consumerList, []byte(name), []byte(strconv.Itoa(c)))
		}

		return protocol.MakeMultiBulkReply([][]byte{
			[]byte(strconv.Itoa(count)),
			[]byte(minID.String()),
			[]byte(maxID.String()),
			protocol.MakeMultiBulkReply(consumerList).ToBytes(),
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
			if err != nil || idle < 0 {
				return protocol.MakeErrReply("ERR Invalid min-idle-time")
			}
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
		idleMs := now.Sub(pending.DeliveryTime).Milliseconds()
		if idleMs < 0 {
			idleMs = 0
		}
		if minIdleMs >= 0 && idleMs < minIdleMs {
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
		idleMs := now.Sub(row.pending.DeliveryTime).Milliseconds()
		if idleMs < 0 {
			idleMs = 0
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
		return protocol.MakeErrReply("ERR no such key")
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
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
		return protocol.MakeIntReply(0)
	}

	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeIntReply(0)
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
		pairs := []redis.Reply{
			protocol.MakeBulkReply([]byte("name")),
			protocol.MakeBulkReply([]byte(group.Name)),
			protocol.MakeBulkReply([]byte("consumers")),
			protocol.MakeIntReply(int64(group.Consumers.Len())),
			protocol.MakeBulkReply([]byte("pending")),
			protocol.MakeIntReply(int64(len(group.Pending))),
			protocol.MakeBulkReply([]byte("last-delivered-id")),
			protocol.MakeBulkReply([]byte(group.LastID.String())),
		}
		if group.EntriesRead >= 0 {
			pairs = append(pairs,
				protocol.MakeBulkReply([]byte("entries-read")),
				protocol.MakeIntReply(group.EntriesRead),
			)
		}
		replies = append(replies, protocol.MakeMultiRawReply(pairs))
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
		return protocol.MakeErrReply("ERR NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
	}

	var replies []redis.Reply
	group.Consumers.ForEach(func(name string, val interface{}) bool {
		c := val.(*stream.Consumer)
		idle := time.Since(c.SeenTime).Milliseconds()
		if c.SeenTime.IsZero() {
			idle = 0
		}
		pairs := []redis.Reply{
			protocol.MakeBulkReply([]byte("name")),
			protocol.MakeBulkReply([]byte(c.Name)),
			protocol.MakeBulkReply([]byte("pending")),
			protocol.MakeIntReply(int64(len(c.Pending))),
			protocol.MakeBulkReply([]byte("idle")),
			protocol.MakeIntReply(idle),
		}
		replies = append(replies, protocol.MakeMultiRawReply(pairs))
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
