package database

import (
	"strconv"
	"strings"

	"github.com/hdt3213/godis/datastruct/stream"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

func (db *DB) getAsStream(key string) (*stream.Stream, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	s, ok := entity.Data.(*stream.Stream)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return s, nil
}

func (db *DB) getOrInitStream(key string) (s *stream.Stream, inited bool, errReply protocol.ErrorReply) {
	s, errReply = db.getAsStream(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if s == nil {
		s = stream.NewStream()
		db.PutEntity(key, &database.DataEntity{
			Data: s,
		})
		inited = true
	}
	return s, inited, nil
}

// execXAdd 将条目添加到Stream
// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|id field value [field value ...]
func execXAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xadd' command")
	}
	
	key := string(args[0])
	var opts stream.AddOptions
	var idStr string
	var fieldArgs [][]byte
	
	// 解析选项
	i := 1
	parseOpts := true
	for i < len(args) && parseOpts {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "NOMKSTREAM":
			opts.NoMkStream = true
			i++
		case "MAXLEN":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i++
			if strings.ToUpper(string(args[i])) == "~" {
				opts.MaxLenApprox = true
				i++
			} else if strings.ToUpper(string(args[i])) == "=" {
				i++
			}
			if i >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			maxlen, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil || maxlen < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			opts.MaxLen = maxlen
			i++
			
			// 解析可选的LIMIT
			if i < len(args) && strings.ToUpper(string(args[i])) == "LIMIT" {
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				limit, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil || limit < 0 {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				opts.Limit = limit
				i += 2
			}
		case "MINID":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i++
			// 跳过可选的 = 或 ~ 标记
			if i < len(args) {
				upper := strings.ToUpper(string(args[i]))
				if upper == "~" || upper == "=" {
					i++
				}
			}
			if i >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			minID, err := stream.ParseStreamID(string(args[i]), stream.StreamID{})
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
			opts.MinID = minID
			i++
			
			// 解析可选的LIMIT
			if i < len(args) && strings.ToUpper(string(args[i])) == "LIMIT" {
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				limit, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil || limit < 0 {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				opts.Limit = limit
				i += 2
			}
		default:
			parseOpts = false
		}
	}
	
	// 剩余参数应该是ID和field-value对
	if i >= len(args) {
		return protocol.MakeSyntaxErrReply()
	}
	
	idStr = string(args[i])
	i++
	
	// 检查field-value对
	if len(args)-i < 2 || (len(args)-i)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for XADD")
	}
	fieldArgs = args[i:]
	
	// 构建fields map
	fields := make(map[string]string)
	for j := 0; j < len(fieldArgs); j += 2 {
		field := string(fieldArgs[j])
		value := string(fieldArgs[j+1])
		fields[field] = value
	}
	
	// 获取或创建stream
	var s *stream.Stream
	var inited bool
	var errReply protocol.ErrorReply
	
	if opts.NoMkStream {
		s, errReply = db.getAsStream(key)
		if errReply != nil {
			return errReply
		}
		if s == nil {
			return &protocol.NullBulkReply{}
		}
	} else {
		s, inited, errReply = db.getOrInitStream(key)
		if errReply != nil {
			return errReply
		}
	}
	
	// 如果stream不存在且NoMkStream为true
	if s == nil {
		return &protocol.NullBulkReply{}
	}
	
	// 添加条目
	id, err := s.Add(idStr, fields, &opts)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	
	// 记录AOF
	if inited {
		// 如果新建了stream，使用特殊的编码
		db.addAof(utils.ToCmdLine3("xadd", args...))
	} else {
		db.addAof(utils.ToCmdLine3("xadd", args...))
	}
	
	return protocol.MakeBulkReply([]byte(id.String()))
}

// execXRange 获取范围内的条目
// XRANGE key start end [COUNT count]
func execXRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xrange' command")
	}
	
	key := string(args[0])
	startStr := string(args[1])
	endStr := string(args[2])
	
	countArg := -1 // 无限制
	if len(args) > 3 {
		if len(args) != 5 || strings.ToUpper(string(args[3])) != "COUNT" {
			return protocol.MakeSyntaxErrReply()
		}
		c, err := strconv.Atoi(string(args[4]))
		if err != nil || c < 0 {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		countArg = c
	}
	
	// 解析start
	var start stream.StreamID
	if startStr == "-" {
		start = stream.StreamID{0, 0} // 最小可能ID
	} else {
		var err error
		start, err = stream.ParseStreamID(startStr, stream.StreamID{})
		if err != nil {
			// 可能是部分ID (只包含时间戳)
			if ts, err2 := strconv.ParseInt(startStr, 10, 64); err2 == nil {
				start = stream.StreamID{Timestamp: ts, Sequence: 0}
			} else {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
		}
	}
	
	// 解析end
	var end stream.StreamID
	if endStr == "+" {
		end = stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1} // 最大可能ID
	} else {
		var err error
		end, err = stream.ParseStreamID(endStr, stream.StreamID{})
		if err != nil {
			// 可能是部分ID (只包含时间戳)
			if ts, err2 := strconv.ParseInt(endStr, 10, 64); err2 == nil {
				end = stream.StreamID{Timestamp: ts, Sequence: 1<<63 - 1}
			} else {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
		}
	}
	
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	entries := s.Range(start, end)
	
	// 应用count限制
	if countArg >= 0 && len(entries) > countArg {
		entries = entries[:countArg]
	}
	
	return streamEntriesToReply(entries)
}

// execXRevRange 反向获取范围内的条目
// XREVRANGE key end start [COUNT count]
func execXRevRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xrevrange' command")
	}
	
	key := string(args[0])
	endStr := string(args[1])
	startStr := string(args[2])
	
	count := -1
	if len(args) > 3 {
		if len(args) != 5 || strings.ToUpper(string(args[3])) != "COUNT" {
			return protocol.MakeSyntaxErrReply()
		}
		c, err := strconv.Atoi(string(args[4]))
		if err != nil || c < 0 {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = c
	}
	
	// 解析end (作为最大值)
	var end stream.StreamID
	if endStr == "+" {
		end = stream.StreamID{Timestamp: 1<<63 - 1, Sequence: 1<<63 - 1}
	} else {
		var err error
		end, err = stream.ParseStreamID(endStr, stream.StreamID{})
		if err != nil {
			if ts, err2 := strconv.ParseInt(endStr, 10, 64); err2 == nil {
				end = stream.StreamID{Timestamp: ts, Sequence: 1<<63 - 1}
			} else {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
		}
	}
	
	// 解析start (作为最小值)
	var start stream.StreamID
	if startStr == "-" {
		start = stream.StreamID{0, 0}
	} else {
		var err error
		start, err = stream.ParseStreamID(startStr, stream.StreamID{})
		if err != nil {
			if ts, err2 := strconv.ParseInt(startStr, 10, 64); err2 == nil {
				start = stream.StreamID{Timestamp: ts, Sequence: 0}
			} else {
				return protocol.MakeErrReply("ERR Invalid stream ID")
			}
		}
	}
	
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	entries := s.ReverseRange(start, end, count)
	return streamEntriesToReply(entries)
}

// execXLen 获取Stream长度
// XLEN key
func execXLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeArgNumErrReply("xlen")
	}
	
	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeIntReply(0)
	}
	
	return protocol.MakeIntReply(int64(s.Len()))
}

// execXDel 删除条目
// XDEL key id [id ...]
func execXDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("xdel")
	}
	
	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeIntReply(0)
	}
	
	ids := make([]stream.StreamID, len(args)-1)
	for i := 1; i < len(args); i++ {
		id, err := stream.ParseStreamID(string(args[i]), stream.StreamID{})
		if err != nil {
			return protocol.MakeErrReply("ERR Invalid stream ID")
		}
		ids[i-1] = id
	}
	
	deleted := s.Delete(ids)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("xdel", args...))
	}
	
	return protocol.MakeIntReply(int64(deleted))
}

// execXGroupCreate 创建消费者组
// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]
func execXGroupCreate(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup' command")
	}
	
	key := string(args[0])
	groupName := string(args[1])
	startID := string(args[2])
	
	mkStream := false
	entriesRead := int64(-1)
	
	// 解析选项
	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "MKSTREAM":
			mkStream = true
		case "ENTRIESREAD":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			er, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			entriesRead = er
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	
	var s *stream.Stream
	var errReply protocol.ErrorReply
	
	if mkStream {
		s, _, errReply = db.getOrInitStream(key)
		if errReply != nil {
			return errReply
		}
	} else {
		s, errReply = db.getAsStream(key)
		if errReply != nil {
			return errReply
		}
		if s == nil {
			return protocol.MakeErrReply("ERR The XGROUP subcommand requires the key to exist.")
		}
	}
	
	err := s.CreateGroup(groupName, startID)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	
	// 设置entriesRead (简化处理)
	if entriesRead >= 0 {
		// 实际应该更新组的EntriesRead字段
	}
	
	db.addAof(utils.ToCmdLine3("xgroup", append([][]byte{[]byte("create")}, args...)...))
	return protocol.MakeOkReply()
}

// execXGroupDestroy 销毁消费者组
// XGROUP DESTROY key groupname
func execXGroupDestroy(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup' command")
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
	
	err := s.DestroyGroup(groupName)
	if err != nil {
		return protocol.MakeIntReply(0)
	}
	
	db.addAof(utils.ToCmdLine3("xgroup", append([][]byte{[]byte("destroy")}, args...)...))
	return protocol.MakeIntReply(1)
}

// execXInfoStream 获取Stream信息
// XINFO STREAM key [FULL [COUNT count]]
func execXInfoStream(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeArgNumErrReply("xinfo")
	}
	
	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR no such key")
	}
	
	full := false
	
	if len(args) > 1 {
		if strings.ToUpper(string(args[1])) == "FULL" {
			full = true
			// COUNT参数处理（简化版，暂不实现详细逻辑）
			if len(args) > 3 && strings.ToUpper(string(args[2])) == "COUNT" {
				_, err := strconv.Atoi(string(args[3]))
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				// 这里可以添加条目数量限制逻辑
			}
		}
	}
	
	info := s.GetInfo()
	
	// 构建回复
	var result [][]byte
	
	// 基本信息
	result = append(result, []byte("length"), []byte(strconv.Itoa(info["length"].(int))))
	result = append(result, []byte("radix-tree-keys"), []byte(strconv.Itoa(info["radix-tree-keys"].(int))))
	result = append(result, []byte("radix-tree-nodes"), []byte(strconv.Itoa(info["radix-tree-nodes"].(int))))
	result = append(result, []byte("last-generated-id"), []byte(info["last-generated-id"].(string)))
	result = append(result, []byte("max-deleted-entry-id"), []byte(info["max-deleted-entry-id"].(string)))
	result = append(result, []byte("entries-added"), []byte(strconv.FormatInt(info["entries-added"].(int64), 10)))
	result = append(result, []byte("recorded-first-entry-id"), []byte(info["recorded-first-entry-id"].(string)))
	
	// 组信息
	groups := s.GetGroups()
	result = append(result, []byte("groups"), []byte(strconv.Itoa(len(groups))))
	
	// 第一个和最后一个条目
	if first, ok := info["first-entry"]; ok {
		if entry, ok := first.(*stream.StreamEntry); ok {
			result = append(result, []byte("first-entry"))
			result = append(result, streamEntryToBytes(entry)...)
		}
	}
	if last, ok := info["last-entry"]; ok {
		if entry, ok := last.(*stream.StreamEntry); ok {
			result = append(result, []byte("last-entry"))
			result = append(result, streamEntryToBytes(entry)...)
		}
	}
	
	if full {
		// 详细信息（简化处理）
		result = append(result, []byte("entries"))
		// 这里可以添加条目列表
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// 辅助函数：将StreamEntry转换为字节数组
func streamEntryToBytes(entry *stream.StreamEntry) [][]byte {
	var fields [][]byte
	for k, v := range entry.Fields {
		fields = append(fields, []byte(k), []byte(v))
	}
	return [][]byte{
		[]byte(entry.ID.String()),
	}	
}

// 辅助函数：将条目列表转换为Redis回复
func streamEntriesToReply(entries []*stream.StreamEntry) redis.Reply {
	if len(entries) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	
	var result [][]byte
	for _, entry := range entries {
		result = append(result, []byte(entry.ID.String()))
		var fields [][]byte
		for k, v := range entry.Fields {
			fields = append(fields, []byte(k), []byte(v))
		}
		result = append(result, fields...)
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// execXTrim 裁剪Stream
// XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]
func execXTrim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xtrim' command")
	}
	
	key := string(args[0])
	strategy := strings.ToUpper(string(args[1]))
	
	var opts stream.AddOptions
	
	switch strategy {
	case "MAXLEN":
		idx := 2
		if strings.ToUpper(string(args[2])) == "~" {
			opts.MaxLenApprox = true
			idx++
		} else if strings.ToUpper(string(args[2])) == "=" {
			idx++
		}
		if idx >= len(args) {
			return protocol.MakeSyntaxErrReply()
		}
		maxlen, err := strconv.ParseInt(string(args[idx]), 10, 64)
		if err != nil || maxlen < 0 {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		opts.MaxLen = maxlen
		idx++
		
		// 解析可选的LIMIT
		if idx < len(args) && strings.ToUpper(string(args[idx])) == "LIMIT" {
			if idx+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			limit, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil || limit < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			opts.Limit = limit
		}
	case "MINID":
		idx := 2
		if strings.ToUpper(string(args[2])) == "~" {
			idx++
		} else if strings.ToUpper(string(args[2])) == "=" {
			idx++
		}
		if idx >= len(args) {
			return protocol.MakeSyntaxErrReply()
		}
		minID, err := stream.ParseStreamID(string(args[idx]), stream.StreamID{})
		if err != nil {
			return protocol.MakeErrReply("ERR Invalid stream ID")
		}
		opts.MinID = minID
		idx++
		
		// 解析可选的LIMIT
		if idx < len(args) && strings.ToUpper(string(args[idx])) == "LIMIT" {
			if idx+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			limit, err := strconv.ParseInt(string(args[idx+1]), 10, 64)
			if err != nil || limit < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			opts.Limit = limit
		}
	default:
		return protocol.MakeSyntaxErrReply()
	}
	
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeIntReply(0)
	}
	
	// 执行裁剪 (这里简化处理，实际需要更好的实现)
	beforeLen := s.Len()
	
	// 重新添加maxlen/minid限制
	if opts.MaxLen > 0 {
		// 创建临时条目触发裁剪
		s.Add("9999999999999-0", map[string]string{"_": "_"}, &opts)
	}
	
	afterLen := s.Len()
	trimmed := beforeLen - afterLen
	
	if trimmed > 0 {
		db.addAof(utils.ToCmdLine3("xtrim", args...))
	}
	
	return protocol.MakeIntReply(int64(trimmed))
}

// execXGroup handles XGROUP subcommands
// XGROUP [CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]] 
//        [DESTROY key groupname] [SETID key groupname id|$]
func execXGroup(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup' command")
	}
	
	subCmd := strings.ToUpper(string(args[0]))
	
	switch subCmd {
	case "CREATE":
		if len(args) < 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup|create' command")
		}
		return execXGroupCreate(db, args[1:])
	case "DESTROY":
		if len(args) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup|destroy' command")
		}
		return execXGroupDestroy(db, args[1:])
	case "SETID":
		if len(args) != 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'xgroup|setid' command")
		}
		return execXGroupSetID(db, args[1:])
	case "HELP":
		return execXGroupHelp()
	default:
		return protocol.MakeErrReply("ERR Unknown XGROUP subcommand '" + subCmd + "'")
	}
}

// execXGroupSetID sets the ID of a consumer group
// XGROUP SETID key groupname id|$
func execXGroupSetID(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	groupName := string(args[1])
	newID := string(args[2])
	
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR no such key")
	}
	
	// Get the group
	group, err := s.GetGroup(groupName)
	if err != nil {
		return protocol.MakeErrReply("ERR No such consumer group '" + groupName + "' for key name '" + key + "'")
	}
	
	// Parse new ID
	var newStreamID stream.StreamID
	if newID == "$" {
		// Use last entry ID
		newStreamID = s.GetLastID()
	} else {
		newStreamID, err = stream.ParseStreamID(newID, stream.StreamID{})
		if err != nil {
			return protocol.MakeErrReply("ERR Invalid stream ID")
		}
	}
	
	// Set new ID
	group.LastID = newStreamID
	
	db.addAof(utils.ToCmdLine3("xgroup", append([][]byte{[]byte("setid")}, args...)...))
	return protocol.MakeOkReply()
}

// execXGroupHelp returns help information
func execXGroupHelp() redis.Reply {
	help := []string{
		"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]",
		"    Create a new consumer group.",
		"DESTROY key groupname",
		"    Remove a consumer group.",
		"SETID key groupname id|$",
		"    Set the current group ID.",
		"HELP",
		"    Print this help.",
	}
	
	result := make([]redis.Reply, len(help))
	for i, h := range help {
		result[i] = protocol.MakeBulkReply([]byte(h))
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("XAdd", execXAdd, writeFirstKey, rollbackFirstKey, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("XRange", execXRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("XRevRange", execXRevRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("XLen", execXLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("XDel", execXDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XTrim", execXTrim, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("XGroup", execXGroup, noPrepare, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 2, 2, 1)
}
