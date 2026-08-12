package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/timewheel"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// wireExpireDict attaches the global time wheel and AOF/notify hooks for field TTL.
func (db *DB) wireExpireDict(hashKey string, ed *dict.ExpireDict) {
	if ed == nil {
		return
	}
	ed.SetTimeWheel(timewheel.Default())
	ed.SetJobPrefix(fmt.Sprintf("hexpire:%d:%s:", db.index, hashKey))
	ed.SetAllowActiveExpire(func() bool { return activeExpireEnabled.Load() })
	// Time-wheel jobs run off the command path: take the DB key write lock
	// (same check-lock-check pattern as key-level ExpireAt).
	ed.SetActiveExpireGuard(func(run func()) {
		keys := []string{hashKey}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		run()
	})
	ed.SetOnExpired(func(field string) {
		db.onHashFieldExpired(hashKey, field)
	})
}

// onHashFieldExpired propagates a TTL-driven field deletion (AOF HDEL + notify).
func (db *DB) onHashFieldExpired(hashKey, field string) {
	if db.addAof != nil {
		db.addAof(utils.ToCmdLine3("hdel", []byte(hashKey), []byte(field)))
	}
	notifyKeyspaceEvent(db, "hdel", hashKey)
	notifyKeyspaceEvent(db, "hexpired", hashKey)

	entity, ok := db.GetEntity(hashKey)
	if !ok {
		return
	}
	ed, ok := entity.Data.(*dict.ExpireDict)
	if !ok {
		return
	}
	if ed.Len() == 0 {
		db.Remove(hashKey)
		removeHashFromIndex(db, hashKey)
		return
	}
	reindexHash(db, hashKey)
}

// getAsExpireDict 获取支持字段级过期的字典
func (db *DB) getAsExpireDict(key string) (*dict.ExpireDict, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}

	// 尝试类型断言为ExpireDict
	if ed, ok := entity.Data.(*dict.ExpireDict); ok {
		db.wireExpireDict(key, ed)
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
		db.wireExpireDict(key, ed)
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
		db.wireExpireDict(key, ed)
		db.PutEntity(key, &database.DataEntity{Data: ed})
		return ed, true, nil
	}
	return ed, false, nil
}

// parseHashFieldsBlock parses "FIELDS numfields field [field ...]" starting at args[i].
// numFieldsErr is the Redis 8.10 message when numfields≤0 but at least one trailing token is present
// (e.g. "ERR invalid number of fields" vs "ERR Number of fields must be a positive integer").
func parseHashFieldsBlock(args [][]byte, i int, cmd, numFieldsErr string) (fields []string, next int, errReply redis.Reply) {
	arity := protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	if i >= len(args) || strings.ToUpper(string(args[i])) != "FIELDS" {
		return nil, i, protocol.MakeSyntaxErrReply()
	}
	if i+1 >= len(args) {
		return nil, i, arity
	}
	n, err := strconv.Atoi(string(args[i+1]))
	if err != nil || n < 1 {
		// Redis: no trailing field tokens → wrong arity; else command-specific numFields ERR.
		if i+2 >= len(args) {
			return nil, i, arity
		}
		return nil, i, protocol.MakeErrReply(numFieldsErr)
	}
	if i+2+n > len(args) {
		return nil, i, arity
	}
	fields = make([]string, n)
	for j := 0; j < n; j++ {
		fields[j] = string(args[i+2+j])
	}
	return fields, i + 2 + n, nil
}

func parseHashExpireOpt(args [][]byte, start int) (expireAt time.Time, persist, keepTTL bool, next int, err redis.Reply) {
	now := time.Now()
	i := start
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		if arg == "FIELDS" {
			break
		}
		switch arg {
		case "EX":
			if i+1 >= len(args) {
				return time.Time{}, false, false, i, protocol.MakeSyntaxErrReply()
			}
			seconds, e := strconv.ParseInt(string(args[i+1]), 10, 64)
			if e != nil || seconds <= 0 {
				return time.Time{}, false, false, i, protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(seconds) * time.Second)
			i += 2
		case "PX":
			if i+1 >= len(args) {
				return time.Time{}, false, false, i, protocol.MakeSyntaxErrReply()
			}
			ms, e := strconv.ParseInt(string(args[i+1]), 10, 64)
			if e != nil || ms <= 0 {
				return time.Time{}, false, false, i, protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = now.Add(time.Duration(ms) * time.Millisecond)
			i += 2
		case "EXAT":
			if i+1 >= len(args) {
				return time.Time{}, false, false, i, protocol.MakeSyntaxErrReply()
			}
			ts, e := strconv.ParseInt(string(args[i+1]), 10, 64)
			if e != nil || ts <= 0 {
				return time.Time{}, false, false, i, protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(ts, 0)
			i += 2
		case "PXAT":
			if i+1 >= len(args) {
				return time.Time{}, false, false, i, protocol.MakeSyntaxErrReply()
			}
			ms, e := strconv.ParseInt(string(args[i+1]), 10, 64)
			if e != nil || ms <= 0 {
				return time.Time{}, false, false, i, protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			expireAt = time.Unix(0, ms*int64(time.Millisecond))
			i += 2
		case "PERSIST":
			persist = true
			i++
		case "KEEPTTL":
			keepTTL = true
			i++
		default:
			return time.Time{}, false, false, i, protocol.MakeSyntaxErrReply()
		}
	}
	return expireAt, persist, keepTTL, i, nil
}

// execHGetEx Redis 8: HGETEX key [EX|PX|EXAT|PXAT|PERSIST] FIELDS numfields field [field ...]
func execHGetEx(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetex' command")
	}
	key := string(args[0])

	// Redis 8 FIELDS form
	hasFields := false
	for _, a := range args[1:] {
		if strings.ToUpper(string(a)) == "FIELDS" {
			hasFields = true
			break
		}
	}
	if hasFields {
		expireAt, persist, _, i, optErr := parseHashExpireOpt(args, 1)
		if optErr != nil {
			return optErr
		}
		fields, _, fieldErr := parseHashFieldsBlock(args, i, "hgetex", "ERR invalid number of fields")
		if fieldErr != nil {
			return fieldErr
		}
		ed, typeErr := db.getAsExpireDict(key)
		if typeErr != nil {
			return typeErr
		}
		out := make([][]byte, len(fields))
		changed := false
		for fi, field := range fields {
			if ed == nil {
				out[fi] = nil
				continue
			}
			val, _, exists := ed.GetWithExpire(field)
			if !exists {
				out[fi] = nil
				continue
			}
			b, _ := val.([]byte)
			out[fi] = b
			if persist {
				ed.Persist(field)
				changed = true
			} else if !expireAt.IsZero() {
				ed.Expire(field, expireAt)
				changed = true
			}
		}
		if changed {
			db.addAof(utils.ToCmdLine3("hgetex", args...))
		}
		return protocol.MakeMultiBulkReply(out)
	}

	return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetex' command")
}

// execHSetEx Redis 8: HSETEX key [FNX|FXX] [EX|PX|EXAT|PXAT|KEEPTTL] FIELDS numfields field value ...
func execHSetEx(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
	}
	key := string(args[0])

	hasFields := false
	for _, a := range args[1:] {
		if strings.ToUpper(string(a)) == "FIELDS" {
			hasFields = true
			break
		}
	}
	if hasFields {
		fnx, fxx := false, false
		i := 1
		for i < len(args) {
			tok := strings.ToUpper(string(args[i]))
			if tok == "FNX" {
				fnx = true
				i++
			} else if tok == "FXX" {
				fxx = true
				i++
			} else {
				break
			}
		}
		if fnx && fxx {
			return protocol.MakeSyntaxErrReply()
		}
		expireAt, _, keepTTL, i2, optErr := parseHashExpireOpt(args, i)
		if optErr != nil {
			return optErr
		}
		i = i2
		if i >= len(args) || strings.ToUpper(string(args[i])) != "FIELDS" {
			return protocol.MakeSyntaxErrReply()
		}
		if i+1 >= len(args) {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
		}
		n, err := strconv.Atoi(string(args[i+1]))
		if err != nil || n < 1 {
			// Redis 8.10 HSETEX: no trailing tokens → wrong arity; else invalid number of fields.
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
			}
			return protocol.MakeErrReply("ERR invalid number of fields")
		}
		// each field has a value → 2*n tokens after numfields
		if i+2+2*n > len(args) {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
		}
		pairs := make([][2][]byte, n)
		for j := 0; j < n; j++ {
			pairs[j][0] = args[i+2+2*j]
			pairs[j][1] = args[i+2+2*j+1]
		}

		ed, _, errReply := db.getOrInitExpireDict(key)
		if errReply != nil {
			return errReply
		}
		if fnx || fxx {
			for _, p := range pairs {
				_, exists := ed.Get(string(p[0]))
				if fnx && exists {
					return protocol.MakeIntReply(0)
				}
				if fxx && !exists {
					return protocol.MakeIntReply(0)
				}
			}
		}
		now := time.Now()
		for _, p := range pairs {
			field := string(p[0])
			exp := expireAt
			if keepTTL {
				_, ttl, exists := ed.GetWithExpire(field)
				if exists && ttl > 0 {
					exp = now.Add(ttl)
				}
			}
			if !exp.IsZero() {
				ed.SetWithExpire(field, p[1], exp.Sub(now))
			} else {
				ed.Set(field, p[1])
			}
		}
		db.addAof(utils.ToCmdLine3("hsetex", args...))
		return protocol.MakeIntReply(1)
	}

	return protocol.MakeErrReply("ERR wrong number of arguments for 'hsetex' command")
}

// execHGetDel Redis 8: HGETDEL key FIELDS numfields field [field ...]
func execHGetDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetdel' command")
	}
	key := string(args[0])
	if strings.ToUpper(string(args[1])) != "FIELDS" {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hgetdel' command")
	}
	fs, _, errReply := parseHashFieldsBlock(args, 1, "hgetdel", "ERR Number of fields must be a positive integer")
	if errReply != nil {
		return errReply
	}
	fields := fs

	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	out := make([][]byte, len(fields))
	if ed == nil {
		return protocol.MakeMultiBulkReply(out)
	}
	deleted := false
	for i, field := range fields {
		val, exists := ed.Get(field)
		if exists {
			b, _ := val.([]byte)
			out[i] = b
			ed.Delete(field)
			deleted = true
		}
	}
	if deleted {
		db.addAof(utils.ToCmdLine3("hgetdel", args...))
	}
	return protocol.MakeMultiBulkReply(out)
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
	cmdName := "httl"
	if millis {
		cmdName = "hpttl"
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmdName + "' command")
	}
	key := string(args[0])
	var fields []string
	multi := false
	if strings.ToUpper(string(args[1])) == "FIELDS" {
		multi = true
		if len(args) < 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmdName + "' command")
		}
		n, err := strconv.Atoi(string(args[2]))
		if err != nil || n < 1 {
			// Redis 8.10: FIELDS 0/-1 with no field tokens → wrong arity;
			// with at least one trailing token → positive integer.
			if len(args) < 4 {
				return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmdName + "' command")
			}
			return protocol.MakeErrReply("ERR Number of fields must be a positive integer")
		}
		if len(args) != 3+n {
			return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmdName + "' command")
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

// execHPersist removes per-field TTLs.
// Redis 8: HPERSIST key FIELDS numfields field [field ...] → array of 1/-1/-2 per field.
func execHPersist(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || strings.ToUpper(string(args[1])) != "FIELDS" {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'hpersist' command")
	}
	fields, _, errReply := parseHashFieldsBlock(args, 1, "hpersist", "ERR Number of fields must be a positive integer")
	if errReply != nil {
		return errReply
	}

	key := string(args[0])
	ed, typeErr := db.getAsExpireDict(key)
	if typeErr != nil {
		return typeErr
	}

	replies := make([]redis.Reply, len(fields))
	changed := false
	for i, field := range fields {
		if ed == nil {
			replies[i] = protocol.MakeIntReply(-2)
			continue
		}
		if _, exists := ed.Get(field); !exists {
			replies[i] = protocol.MakeIntReply(-2)
			continue
		}
		if _, hasTTL := ed.GetExpireTime(field); !hasTTL {
			replies[i] = protocol.MakeIntReply(-1)
			continue
		}
		if ed.Persist(field) {
			replies[i] = protocol.MakeIntReply(1)
			changed = true
		} else {
			// Race/expire between GetExpireTime and Persist
			replies[i] = protocol.MakeIntReply(-1)
		}
	}

	if changed {
		db.addAof(utils.ToCmdLine3("hpersist", args...))
		reindexHash(db, key)
	}

	return protocol.MakeMultiRawReply(replies)
}

// hExpireFlags holds optional NX/XX/GT/LT modifiers
type hExpireFlags struct {
	nx bool
	xx bool
	gt bool
	lt bool
}

func parseHExpireFlags(args [][]byte, cmd string) (flags hExpireFlags, fields [][]byte, errReply redis.Reply) {
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
		return flags, nil, protocol.MakeErrReply("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
	}
	n, err := strconv.Atoi(string(args[i]))
	if err != nil || n < 1 {
		// Redis: FIELDS 0 / FIELDS -1 with no field tokens → wrong arity;
		// with at least one trailing token → Parameter `numFields`…
		if i+1 >= len(args) {
			return flags, nil, protocol.MakeErrReply("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
		}
		return flags, nil, protocol.MakeErrReply("ERR Parameter `numFields` should be greater than 0")
	}
	i++
	if len(args) != i+n {
		return flags, nil, protocol.MakeErrReply("ERR wrong number of arguments for '" + strings.ToLower(cmd) + "' command")
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

func execHExpireFamily(db *DB, args [][]byte, cmd string, at bool, unit time.Duration) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}

	key := string(args[0])
	rawTTL, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	// Redis 8: TTL/timestamp must be >= 0; 0 means expire-now (delete field).
	if rawTTL < 0 {
		return protocol.MakeErrReply("ERR invalid expire time, must be >= 0")
	}

	flags, fields, errReply := parseHExpireFlags(args[2:], cmd)
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
	mutated := false // tracks whether any field was deleted or had its TTL set
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
			mutated = true // field content removed → needs reindex
			results[i] = protocol.MakeIntReply(2)
			continue
		}

		if ed.Expire(field, expireAt) {
			mutated = true // TTL change re-evaluated against index per Redis
			results[i] = protocol.MakeIntReply(1)
		} else {
			results[i] = protocol.MakeIntReply(0)
		}
	}

	// Field-level TTL changes trigger a hash reindex: a field may have been
	// deleted (content change) or its TTL now affects FILTER/score evaluation.
	// Persist the client command when anything mutated (align HGETEX/HPERSIST);
	// Redis filters to successful fields only — Godis keeps the original line.
	if mutated {
		if db.addAof != nil {
			db.addAof(utils.ToCmdLine3(cmd, args...))
		}
		if ed.Len() == 0 {
			db.Remove(key)
			removeHashFromIndex(db, key)
		} else {
			reindexHash(db, key)
		}
	}

	return protocol.MakeMultiRawReply(results)
}

// execHExpire sets expiration on hash fields in seconds
func execHExpire(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, "hexpire", false, time.Second)
}

// execHPExpire sets expiration on hash fields in milliseconds
func execHPExpire(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, "hpexpire", false, time.Millisecond)
}

// execHExpireAt sets absolute expiration on hash fields in unix seconds
func execHExpireAt(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, "hexpireat", true, time.Second)
}

// execHPExpireAt sets absolute expiration on hash fields in unix milliseconds
func execHPExpireAt(db *DB, args [][]byte) redis.Reply {
	return execHExpireFamily(db, args, "hpexpireat", true, time.Millisecond)
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
	_, fields, _ := parseHExpireFlags(args[2:], "hexpire")
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
				strconv.FormatInt(expireAtMs, 10), "FIELDS", "1", field))
		}
	}
	return undoCmdLines
}

func undoHGetEx(db *DB, args [][]byte) []CmdLine {
	if len(args) < 2 {
		return nil
	}
	key := string(args[0])
	var fields []string
	hasFields := false
	for _, a := range args[1:] {
		if strings.ToUpper(string(a)) == "FIELDS" {
			hasFields = true
			break
		}
	}
	if hasFields {
		i := 1
		for i < len(args) && strings.ToUpper(string(args[i])) != "FIELDS" {
			tok := strings.ToUpper(string(args[i]))
			switch tok {
			case "EX", "PX", "EXAT", "PXAT":
				i += 2
			case "PERSIST":
				i++
			default:
				i++
			}
		}
		fs, _, err := parseHashFieldsBlock(args, i, "hgetex", "ERR invalid number of fields")
		if err != nil {
			return nil
		}
		fields = fs
	} else {
		return nil
	}

	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil || ed == nil {
		return rollbackGivenKeys(db, key)
	}
	var undoCmdLines []CmdLine
	for _, field := range fields {
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
				strconv.FormatInt(expireAtMs, 10), "FIELDS", "1", field))
		}
	}
	return undoCmdLines
}

func undoHGetDel(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	if len(args) < 2 || strings.ToUpper(string(args[1])) != "FIELDS" {
		return nil
	}
	fs, _, err := parseHashFieldsBlock(args, 1, "hgetdel", "ERR Number of fields must be a positive integer")
	if err != nil {
		return nil
	}
	return rollbackHashFields(db, key, fs...)
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
	registerCommand("HPersist", execHPersist, writeFirstKey, nil, -5, flagWrite).
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
