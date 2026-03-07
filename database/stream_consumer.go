package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/stream"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
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
	
	// 执行读取
	startTime := time.Now()
	for {
		var result [][]byte
		hasData := false
		
		for j, key := range keys {
			s, errReply := db.getAsStream(key)
			if errReply != nil {
				return errReply
			}
			if s == nil {
				continue
			}
			
			// 解析起始ID
			var startID stream.StreamID
			if ids[j] == "$" {
				// 使用最后一个ID，只读取新数据
				startID = s.GetLastID()
			} else {
				var err error
				startID, err = stream.ParseStreamID(ids[j], stream.StreamID{})
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid stream ID")
				}
			}
			
			// 读取数据（不包含startID本身）
			entries := s.Range(startID, stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1})
			
			// 过滤掉startID本身
			var filtered []*stream.StreamEntry
			for _, entry := range entries {
				if entry.ID.Compare(startID) > 0 {
					filtered = append(filtered, entry)
				}
			}
			
			// 应用count限制
			if count > 0 && len(filtered) > count {
				filtered = filtered[:count]
			}
			
			if len(filtered) > 0 {
				hasData = true
				// 构建该stream的回复
				streamResult := streamEntriesToMultiBulk(filtered)
				result = append(result, []byte(key))
				result = append(result, streamResult...)
			}
		}
		
		if hasData {
			return protocol.MakeMultiBulkReply(result)
		}
		
		// 没有数据
		if blockTimeout == 0 {
			// BLOCK 0 表示无限阻塞直到有数据
			time.Sleep(100 * time.Millisecond)
			continue
		} else if blockTimeout > 0 {
			// 检查是否超时
			if time.Since(startTime) >= blockTimeout {
				return &protocol.NullBulkReply{}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		// 非阻塞模式
		return &protocol.NullBulkReply{}
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
	
	startTime := time.Now()
	for {
		var result [][]byte
		hasData := false
		
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
				// 从历史pending中读取（用于重新读取已递送但未确认的消息）
				startID, err := stream.ParseStreamID(id, stream.StreamID{})
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid stream ID")
				}
				
						// 从消费者的pending中查找
				for pid, pending := range consumer.Pending {
					if pid.Compare(startID) >= 0 {
						// 简化处理
						_ = pending
					}
				}
			}
			
			// 应用count限制
			if count > 0 && len(entries) > count {
				entries = entries[:count]
			}
			
			if len(entries) > 0 {
				hasData = true
				
				// 添加到pending（除非NOACK）
				if !noAck {
					now := time.Now()
					for _, entry := range entries {
						consumer.Pending[entry.ID] = &stream.PendingEntry{
							ID:            entry.ID,
							Consumer:      consumerName,
							DeliveryTime:  now,
							DeliveryCount: 1,
						}
						group.Pending[entry.ID] = consumer.Pending[entry.ID]
					}
				}
				
				streamResult := streamEntriesToMultiBulk(entries)
				result = append(result, []byte(key))
				result = append(result, streamResult...)
			}
		}
		
		if hasData {
			return protocol.MakeMultiBulkReply(result)
		}
		
		if blockTimeout == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if blockTimeout > 0 {
			if time.Since(startTime) >= blockTimeout {
				return &protocol.NullBulkReply{}
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		return &protocol.NullBulkReply{}
	}
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
	
	// 详细模式：返回具体条目
	if len(args) < 5 {
		return protocol.MakeSyntaxErrReply()
	}
	
	startID, err := stream.ParseStreamID(string(args[2]), stream.StreamID{})
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid stream ID")
	}
	
	endID, err := stream.ParseStreamID(string(args[3]), stream.StreamID{})
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid stream ID")
	}
	
	count, err := strconv.Atoi(string(args[4]))
	if err != nil || count < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	
	var consumerFilter string
	if len(args) >= 6 {
		consumerFilter = string(args[5])
	}
	
	// 收集pending条目
	var result [][]byte
	collected := 0
	
	for id, pending := range group.Pending {
		if id.Compare(startID) < 0 || id.Compare(endID) > 0 {
			continue
		}
		
		if consumerFilter != "" && pending.Consumer != consumerFilter {
			continue
		}
		
		if collected >= count {
			break
		}
		
		deliveryTimeMs := pending.DeliveryTime.UnixMilli()
		result = append(result, []byte(id.String()))
		result = append(result, []byte(pending.Consumer))
		result = append(result, []byte(strconv.FormatInt(deliveryTimeMs, 10)))
		result = append(result, []byte(strconv.Itoa(pending.DeliveryCount)))
		
		collected++
	}
	
	return protocol.MakeMultiBulkReply(result)
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
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xinfo' command")
	}
	
	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	groups := s.GetGroups()
	
	var result [][]byte
	for _, group := range groups {
		groupInfo := [][]byte{
			[]byte("name"),
			[]byte(group.Name),
			[]byte("consumers"),
			[]byte(strconv.Itoa(group.Consumers.Len())),
			[]byte("pending"),
			[]byte(strconv.Itoa(len(group.Pending))),
			[]byte("last-delivered-id"),
			[]byte(group.LastID.String()),
		}
		if group.EntriesRead >= 0 {
			groupInfo = append(groupInfo, []byte("entries-read"), []byte(strconv.FormatInt(group.EntriesRead, 10)))
		}
		result = append(result, protocol.MakeMultiBulkReply(groupInfo).ToBytes())
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// 辅助函数：将条目列表转换为MultiBulk回复格式
func streamEntriesToMultiBulk(entries []*stream.StreamEntry) [][]byte {
	var result [][]byte
	for _, entry := range entries {
		var fields [][]byte
		for k, v := range entry.Fields {
			fields = append(fields, []byte(k), []byte(v))
		}
		entryResult := [][]byte{
			[]byte(entry.ID.String()),
			protocol.MakeMultiBulkReply(fields).ToBytes(),
		}
		result = append(result, protocol.MakeMultiBulkReply(entryResult).ToBytes())
	}
	return result
}

func init() {
	registerCommand("XRead", execXRead, noPrepare, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagBlocking}, 1, 1, 1)
	registerCommand("XReadGroup", execXReadGroup, noPrepare, nil, -6, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagBlocking}, 1, 1, 1)
	registerCommand("XAck", execXAck, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XPending", execXPending, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("XGroupCreateConsumer", execXGroupCreateConsumer, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XGroupDelConsumer", execXGroupDelConsumer, writeFirstKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XInfoGroups", execXInfoGroups, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
