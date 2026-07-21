package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// getAsExpireDict 获取支持字段级过期的字典
func (db *DB) getAsExpireDict(key string) (*dict.ExpireDict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}

	// 尝试类型断言为ExpireDict
	if ed, ok := entity.Data.(*dict.ExpireDict); ok {
		return ed, nil
	}

	// 尝试类型断言为普通Dict，需要迁移
	if d, ok := entity.Data.(dict.Dict); ok {
		// 创建新的ExpireDict并迁移数据
		ed := dict.NewExpireDict(16)
		d.ForEach(func(key string, val interface{}) bool {
			ed.Set(key, val)
			return true
		})
		// 更新实体
		db.PutEntity(key, &database.DataEntity{Data: ed})
		return ed, nil
	}

	return nil, &protocol.WrongTypeErrReply{}
}

// getOrInitExpireDict 获取或初始化支持字段级过期的字典
func (db *DB) getOrInitExpireDict(key string) (*dict.ExpireDict, bool, protocol.ErrorReply) {
	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	if ed == nil {
		ed = dict.NewExpireDict(16)
		db.PutEntity(key, &database.DataEntity{Data: ed})
		return ed, true, nil
	}
	return ed, false, nil
}

// execHGetEx 获取字段值并可选择设置过期时间
// HGETEX key field [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|PERSIST]
func execHGetEx(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetex' command")
	}

	key := string(args[0])
	field := string(args[1])

	// 获取hash
	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	if ed == nil {
		return &protocol.NullBulkReply{}
	}

	// 获取字段值
	val, _, exists := ed.GetWithExpire(field)
	if !exists {
		return &protocol.NullBulkReply{}
	}

	// 解析过期选项
	now := time.Now()
	expireAt := time.Time{} // 零值表示不设置过期
	persist := false

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "EX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			seconds, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || seconds <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(seconds) * time.Second)
			i++
		case "PX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(ms) * time.Millisecond)
			i++
		case "EXAT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ts, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ts <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(ts, 0)
			i++
		case "PXAT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(0, ms*int64(time.Millisecond))
			i++
		case "PERSIST":
			persist = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// 应用过期设置
	if persist {
		ed.Persist(field)
		db.addAof(utils.ToCmdLine3("hgetex", args...))
	} else if !expireAt.IsZero() {
		ed.Expire(field, expireAt)
		db.addAof(utils.ToCmdLine3("hgetex", args...))
	}

	// 返回字段值
	value, _ := val.([]byte)
	return protocol.MakeBulkReply(value)
}

// execHSetEx 设置字段值并可选择设置过期时间
// HSETEX key field value [EX seconds|PX milliseconds|EXAT timestamp|PXAT milliseconds-timestamp|KEEPTTL]
func execHSetEx(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
	}

	key := string(args[0])
	field := string(args[1])
	value := args[2]

	// 获取或创建hash
	ed, _, errReply := db.getOrInitExpireDict(key)
	if errReply != nil {
		return errReply
	}

	// 解析过期选项
	now := time.Now()
	expireAt := time.Time{}
	keepTTL := false

	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "EX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			seconds, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || seconds <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(seconds) * time.Second)
			i++
		case "PX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(ms) * time.Millisecond)
			i++
		case "EXAT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ts, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ts <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(ts, 0)
			i++
		case "PXAT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(0, ms*int64(time.Millisecond))
			i++
		case "KEEPTTL":
			keepTTL = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// 检查是否需要保持原有TTL
	if keepTTL {
		_, ttl, exists := ed.GetWithExpire(field)
		if exists && ttl > 0 {
			expireAt = now.Add(ttl)
		}
	}

	// 设置字段
	if !expireAt.IsZero() {
		ed.SetWithExpire(field, value, expireAt.Sub(now))
	} else {
		ed.Set(field, value)
	}

	db.addAof(utils.ToCmdLine3("hsetex", args...))
	return protocol.MakeIntReply(1)
}

// execHGetDel 获取并删除字段
// HGETDEL key field [field ...]
func execHGetDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetdel' command")
	}

	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}

	// 获取hash
	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	if ed == nil {
		return &protocol.NullBulkReply{}
	}

	// 如果只查询一个字段，返回该字段的值
	if len(fields) == 1 {
		val, exists := ed.Get(fields[0])
		if !exists {
			return &protocol.NullBulkReply{}
		}
		// 删除字段
		ed.Delete(fields[0])
		db.addAof(utils.ToCmdLine3("hgetdel", args...))

		value, _ := val.([]byte)
		return protocol.MakeBulkReply(value)
	}

	// 多个字段，返回数组
	result := make([][]byte, len(fields))
	for i, field := range fields {
		val, exists := ed.Get(field)
		if exists {
			value, _ := val.([]byte)
			result[i] = value
			ed.Delete(field)
		}
	}

	db.addAof(utils.ToCmdLine3("hgetdel", args...))
	return protocol.MakeMultiBulkReply(result)
}

// execHTTL 获取字段剩余生存时间
// HTTL key field  |  HTTL key FIELDS numfields field [field ...]
func execHTTL(db *DB, args [][]byte) redis.Reply {
	return execHTTLFamily(db, args, false)
}

// execHPTTL 获取字段剩余生存时间（毫秒）
func execHPTTL(db *DB, args [][]byte) redis.Reply {
	return execHTTLFamily(db, args, true)
}

func execHTTLFamily(db *DB, args [][]byte, millis bool) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	key := string(args[0])
	var fields []string
	multi := false
	if strings.ToUpper(string(args[1])) == "FIELDS" {
		multi = true
		if len(args) < 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		n, err := strconv.Atoi(string(args[2]))
		if err != nil || n < 1 {
			return protocol.MakeErrReply("ERR Number of fields can't be negative or zero")
		}
		if len(args) != 3+n {
			return protocol.MakeErrReply("ERR wrong number of arguments")
		}
		fields = make([]string, n)
		for i := 0; i < n; i++ {
			fields[i] = string(args[3+i])
		}
	} else if len(args) == 2 {
		fields = []string{string(args[1])}
	} else {
		return protocol.MakeSyntaxErrReply()
	}

	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	replies := make([]redis.Reply, len(fields))
	for i, field := range fields {
		if ed == nil {
			replies[i] = protocol.MakeIntReply(-2)
			continue
		}
		if millis {
			replies[i] = protocol.MakeIntReply(ed.PTTL(field))
		} else {
			replies[i] = protocol.MakeIntReply(ed.TTL(field))
		}
	}
	if multi {
		return protocol.MakeMultiRawReply(replies)
	}
	return replies[0]
}

// execHPersist 移除字段的过期时间
// HPERSIST key field [field ...]
func execHPersist(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeArgNumErrReply("hpersist")
	}

	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i])
	}

	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	if ed == nil {
		return protocol.MakeIntReply(0)
	}

	persisted := 0
	for _, field := range fields {
		if ed.Persist(field) {
			persisted++
		}
	}

	if persisted > 0 {
		db.addAof(utils.ToCmdLine3("hpersist", args...))
	}

	return protocol.MakeIntReply(int64(persisted))
}

// hExpireFlags holds optional NX/XX/GT/LT modifiers
type hExpireFlags struct {
	nx bool
	xx bool
	gt bool
	lt bool
}

func parseHExpireFlags(args [][]byte) (flags hExpireFlags, fields [][]byte, errReply redis.Reply) {
	i := 0
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "NX":
			flags.nx = true
			i++
		case "XX":
			flags.xx = true
			i++
		case "GT":
			flags.gt = true
			i++
		case "LT":
			flags.lt = true
			i++
		case "FIELDS":
			goto fields
		default:
			return flags, nil, protocol.MakeSyntaxErrReply()
		}
	}
fields:
	if i >= len(args) || strings.ToUpper(string(args[i])) != "FIELDS" {
		return flags, nil, protocol.MakeSyntaxErrReply()
	}
	i++
	if i >= len(args) {
		return flags, nil, protocol.MakeErrReply("ERR wrong number of arguments")
	}
	n, err := strconv.Atoi(string(args[i]))
	if err != nil || n < 1 {
		return flags, nil, protocol.MakeErrReply("ERR Number of fields can't be negative or zero")
	}
	i++
	if len(args) != i+n {
		return flags, nil, protocol.MakeErrReply("ERR wrong number of arguments")
	}
	fields = args[i:]
	return
}

func validateHExpireFlags(flags hExpireFlags) redis.Reply {
	count := 0
	if flags.nx {
		count++
	}
	if flags.xx {
		count++
	}
	if flags.gt {
		count++
	}
	if flags.lt {
		count++
	}
	if count > 1 {
		return protocol.MakeErrReply("ERR NX, XX, GT, and LT options are mutually exclusive")
	}
	return nil
}

func execHExpireFamily(db *DB, args [][]byte, at bool, unit time.Duration) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}

	key := string(args[0])
	rawTTL, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || rawTTL <= 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	flags, fields, errReply := parseHExpireFlags(args[2:])
	if errReply != nil {
		return errReply
	}
	if len(fields) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	if reply := validateHExpireFlags(flags); reply != nil {
		return reply
	}

	var expireAt time.Time
	if at {
		if unit == time.Second {
			expireAt = time.Unix(rawTTL, 0)
		} else {
			expireAt = time.Unix(0, rawTTL*int64(time.Millisecond))
		}
	} else {
		expireAt = time.Now().Add(time.Duration(rawTTL) * unit)
	}

	ed, errReply2 := db.getAsExpireDict(key)
	if errReply2 != nil {
		return errReply2
	}
	if ed == nil {
		// key does not exist: every field is reported as missing
		replies := make([]redis.Reply, len(fields))
		for i := range fields {
			replies[i] = protocol.MakeIntReply(-2)
		}
		return protocol.MakeMultiRawReply(replies)
	}

	results := make([]redis.Reply, len(fields))
	now := time.Now()
	for i, fieldBytes := range fields {
		field := string(fieldBytes)
		_, remaining, exists := ed.GetWithExpire(field)
		if !exists {
			results[i] = protocol.MakeIntReply(-2)
			continue
		}

		hasExpire := remaining >= 0
		var currentExpire time.Time
		if hasExpire {
			currentExpire = now.Add(remaining)
		}

		// condition checks
		if flags.nx && hasExpire {
			results[i] = protocol.MakeIntReply(0)
			continue
		}
		if flags.xx && !hasExpire {
			results[i] = protocol.MakeIntReply(0)
			continue
		}
		if flags.gt {
			// no TTL ≈ +inf → GT always ok
			if hasExpire && !expireAt.After(currentExpire) {
				results[i] = protocol.MakeIntReply(0)
				continue
			}
		}
		if flags.lt {
			// no TTL ≈ +inf → LT never ok
			if !hasExpire || !expireAt.Before(currentExpire) {
				results[i] = protocol.MakeIntReply(0)
				continue
			}
		}

		if !expireAt.After(now) {
			ed.Delete(field)
			results[i] = protocol.MakeIntReply(2)
			continue
		}

		if ed.Expire(field, expireAt) {
			results[i] = protocol.MakeIntReply(1)
		} else {
			results[i] = protocol.MakeIntReply(0)
		}
	}

	return protocol.MakeMultiRawReply(results)
}

// execHExpire sets expiration on hash fields in seconds
func execHExpire(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, false, time.Second)
}

// execHPExpire sets expiration on hash fields in milliseconds
func execHPExpire(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, false, time.Millisecond)
}

// execHExpireAt sets absolute expiration on hash fields in unix seconds
func execHExpireAt(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, true, time.Second)
}

// execHPExpireAt sets absolute expiration on hash fields in unix milliseconds
func execHPExpireAt(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, true, time.Millisecond)
}

func undoHExpire(db *DB, args [][]byte) []CmdLine {
	if len(args) < 3 {
		return nil
	}
	key := string(args[0])
	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil || ed == nil {
		return rollbackGivenKeys(db, key)
	}

	var undoCmdLines []CmdLine
	// args[1] is the TTL; args[2:] may contain flags before fields
	_, fields, _ := parseHExpireFlags(args[2:])
	for _, fieldBytes := range fields {
		field := string(fieldBytes)
		val, remaining, exists := ed.GetWithExpire(field)
		if !exists {
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("HDEL", key, field))
			continue
		}
		value, _ := val.([]byte)
		undoCmdLines = append(undoCmdLines, utils.ToCmdLine("HSET", key, field, string(value)))
		if remaining >= 0 {
			expireAtMs := time.Now().Add(remaining).UnixNano() / int64(time.Millisecond)
			undoCmdLines = append(undoCmdLines, utils.ToCmdLine("HPEXPIREAT", key,
				strconv.FormatInt(expireAtMs, 10), field))
		}
	}
	return undoCmdLines
}

func undoHGetEx(db *DB, args [][]byte) []CmdLine {
	// HGETEX的回滚比较复杂，因为它可能修改了过期时间
	// 简化处理：只回滚字段值（如果字段被删除）
	if len(args) < 2 {
		return nil
	}
	key := string(args[0])
	field := string(args[1])
	return rollbackHashFields(db, key, field)
}

func undoHGetDel(db *DB, args [][]byte) []CmdLine {
	// HGETDEL删除字段，回滚需要恢复字段值
	// 由于值已经丢失，这里简化处理
	return nil
}

func init() {
	registerCommand("HGetEx", execHGetEx, writeFirstKey, undoHGetEx, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HSetEx", execHSetEx, writeFirstKey, undoHSet, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("HGetDel", execHGetDel, writeFirstKey, undoHGetDel, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HTTL", execHTTL, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HPTTL", execHPTTL, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HPersist", execHPersist, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)

	registerCommand("HExpire", execHExpire, writeFirstKey, undoHExpire, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HPExpire", execHPExpire, writeFirstKey, undoHExpire, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HExpireAt", execHExpireAt, writeFirstKey, undoHExpire, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("HPExpireAt", execHPExpireAt, writeFirstKey, undoHExpire, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
}
