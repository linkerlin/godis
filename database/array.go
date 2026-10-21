package database

// Redis 8.8 Array preview subset (R5-19): ARSET/ARMSET/ARGET/ARMGET/
// ARGETRANGE/ARLEN/ARCOUNT/ARDEL/ARDELRANGE, extended by ARINSERT/ARNEXT/
// ARSEEK/ARINFO/ARLASTITEMS/AROP/ARGREP/ARRING/ARSCAN. TYPE reports "array";
// persistence goes through the Godis opaque envelope.

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/array"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

func (db *DB) getAsArray(key string) (*array.Array, redis.Reply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	arr, ok := entity.Data.(*array.Array)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return arr, nil
}

func (db *DB) getOrInitArray(key string) (*array.Array, bool, redis.Reply) {
	arr, errReply := db.getAsArray(key)
	if errReply != nil {
		return nil, false, errReply
	}
	if arr != nil {
		return arr, false, nil
	}
	arr = array.New()
	db.PutEntity(key, &database.DataEntity{Data: arr})
	return arr, true, nil
}

// execARSet ARSET key index value [value ...] — contiguous writes,
// returns the number of new (previously empty) slots filled.
func execARSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arset' command")
	}
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || index < 0 || index+int64(len(args)-3) > array.MaxIndex {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	fresh := 0
	for i, v := range args[2:] {
		fresh += arr.Set(index+int64(i), v)
	}
	db.addAof(utils.ToCmdLine3("arset", args...))
	return protocol.MakeIntReply(int64(fresh))
}

// execARMSet ARMSET key index value [index value ...] — sparse pairs,
// returns the number of new slots filled.
func execARMSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'armset' command")
	}
	type pair struct {
		idx int64
		val []byte
	}
	pairs := make([]pair, 0, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		idx, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil || idx < 0 || idx > array.MaxIndex {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		pairs = append(pairs, pair{idx, args[i+1]})
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	fresh := 0
	for _, p := range pairs {
		fresh += arr.Set(p.idx, p.val)
	}
	db.addAof(utils.ToCmdLine3("armset", args...))
	return protocol.MakeIntReply(int64(fresh))
}

// execARGet ARGET key index — bulk or nil for missing key/index/empty slot.
func execARGet(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arget' command")
	}
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return &protocol.NullBulkReply{}
	}
	if v, ok := arr.Get(index); ok {
		return protocol.MakeBulkReply(v)
	}
	return &protocol.NullBulkReply{}
}

// execARMGet ARMGET key index [index ...] — nil for unset indices.
func execARMGet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'armget' command")
	}
	indices := make([]int64, 0, len(args)-1)
	for _, a := range args[1:] {
		idx, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		indices = append(indices, idx)
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	result := make([]redis.Reply, len(indices))
	for i, idx := range indices {
		if arr != nil {
			if v, ok := arr.Get(idx); ok {
				result[i] = protocol.MakeBulkReply(v)
				continue
			}
		}
		result[i] = &protocol.NullBulkReply{}
	}
	return protocol.MakeMultiRawReply(result)
}

// execARGetRange ARGETRANGE key start end — inclusive; start>end reverses.
func execARGetRange(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'argetrange' command")
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	slots := arr.Range(start, end)
	result := make([]redis.Reply, len(slots))
	for i, s := range slots {
		if s == nil {
			result[i] = &protocol.NullBulkReply{}
		} else {
			result[i] = protocol.MakeBulkReply(s)
		}
	}
	return protocol.MakeMultiRawReply(result)
}

// execARLen ARLEN key — max index + 1, 0 when missing.
func execARLen(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arlen' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(arr.Len())
}

// execARCount ARCOUNT key — number of non-empty slots, 0 when missing.
func execARCount(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arcount' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(arr.Count())
}

// execARDel ARDEL key index [index ...] — count of non-empty slots deleted.
func execARDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ardel' command")
	}
	indices := make([]int64, 0, len(args)-1)
	for _, a := range args[1:] {
		idx, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		indices = append(indices, idx)
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := arr.Del(indices...)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("ardel", args...))
	}
	return protocol.MakeIntReply(deleted)
}

// execARDelRange ARDELRANGE key start end [start end ...] — inclusive
// ranges, start>end normalized, overlaps counted once.
func execARDelRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ardelrange' command")
	}
	ranges := make([][2]int64, 0, (len(args)-1)/2)
	for i := 1; i+1 < len(args); i += 2 {
		lo, err := strconv.ParseInt(string(args[i]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		hi, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		ranges = append(ranges, [2]int64{lo, hi})
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	deleted := arr.DelRanges(ranges)
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("ardelrange", args...))
	}
	return protocol.MakeIntReply(deleted)
}

// ---- R5-19 extension: ARINSERT/ARNEXT/ARSEEK/ARINFO/ARLASTITEMS/AROP/ARGREP/ARRING ----

// execARInsert ARINSERT key value [value ...] — writes at the insertion cursor
// and returns the last index used.
func execARInsert(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arinsert' command")
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	last, ok := arr.Insert(args[1:]...)
	if !ok {
		return protocol.MakeErrReply("ERR array insert cursor exhausted")
	}
	db.addAof(utils.ToCmdLine3("arinsert", args...))
	return protocol.MakeIntReply(last)
}

// execARNext ARNEXT key — next index ARINSERT would use; null when exhausted.
func execARNext(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arnext' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	next, ok := arr.NextIndex()
	if !ok {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeIntReply(next)
}

// execARSeek ARSEEK key index — sets the ARINSERT/ARRING cursor.
func execARSeek(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arseek' command")
	}
	index, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || index < 0 || index > array.MaxIndex {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeIntReply(0)
	}
	arr.SetCursor(index)
	db.addAof(utils.ToCmdLine3("arseek", args...))
	return protocol.MakeIntReply(1)
}

// execARInfo ARINFO key [FULL] — array metadata.
func execARInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arinfo' command")
	}
	if len(args) == 2 && !strings.EqualFold(string(args[1]), "FULL") {
		return protocol.MakeSyntaxErrReply()
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	next, hasNext := arr.NextIndex()
	m := protocol.MakeMapReply()
	m.Put("count", protocol.MakeIntReply(arr.Count()))
	m.Put("len", protocol.MakeIntReply(arr.Len()))
	if hasNext {
		m.Put("next-insert-index", protocol.MakeIntReply(next))
	} else {
		m.Put("next-insert-index", &protocol.NullBulkReply{})
	}
	slices := int64(0)
	if arr.Len() > 0 {
		slices = 1
	}
	m.Put("slices", protocol.MakeIntReply(slices))
	m.Put("directory-size", protocol.MakeIntReply(slices))
	m.Put("super-dir-entries", protocol.MakeIntReply(0))
	m.Put("slice-size", protocol.MakeIntReply(4096))
	return m
}

// execARLastItems ARLASTITEMS key count [REV] — most recently inserted values.
func execARLastItems(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arlastitems' command")
	}
	count, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	rev := false
	if len(args) == 3 {
		if !strings.EqualFold(string(args[2]), "REV") {
			return protocol.MakeSyntaxErrReply()
		}
		rev = true
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil || count <= 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	vals := arr.LastItems(count, rev)
	replies := make([]redis.Reply, len(vals))
	for i, v := range vals {
		if v == nil {
			replies[i] = &protocol.NullBulkReply{}
			continue
		}
		replies[i] = protocol.MakeBulkReply(v)
	}
	return protocol.MakeMultiRawReply(replies)
}

// execAROp AROP key start end <SUM|MIN|MAX|AND|OR|XOR|MATCH value|USED>
func execAROp(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arop' command")
	}
	start, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	end, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	op := strings.ToUpper(string(args[3]))
	needsValue := op == "MATCH"
	if needsValue && len(args) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arop' command")
	}
	if !needsValue && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arop' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	vals := aropRange(arr, start, end)
	switch op {
	case "SUM", "MIN", "MAX":
		var nums []float64
		for _, v := range vals {
			if v == nil {
				continue
			}
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				continue
			}
			nums = append(nums, f)
		}
		if len(nums) == 0 {
			return &protocol.NullBulkReply{}
		}
		result := nums[0]
		for _, f := range nums[1:] {
			switch op {
			case "SUM":
				result += f
			case "MIN":
				if f < result {
					result = f
				}
			case "MAX":
				if f > result {
					result = f
				}
			}
		}
		return protocol.MakeBulkReply([]byte(strconv.FormatFloat(result, 'f', -1, 64)))
	case "AND", "OR", "XOR":
		var acc int64
		found := false
		for _, v := range vals {
			if v == nil {
				continue
			}
			f, err := strconv.ParseFloat(string(v), 64)
			if err != nil {
				continue
			}
			n := int64(f) // truncate toward zero
			if !found {
				acc = n
				found = true
				continue
			}
			switch op {
			case "AND":
				acc &= n
			case "OR":
				acc |= n
			case "XOR":
				acc ^= n
			}
		}
		if !found {
			return &protocol.NullBulkReply{}
		}
		return protocol.MakeIntReply(acc)
	case "MATCH":
		target := string(args[4])
		var n int64
		for _, v := range vals {
			if v != nil && string(v) == target {
				n++
			}
		}
		return protocol.MakeIntReply(n)
	case "USED":
		var n int64
		for _, v := range vals {
			if v != nil {
				n++
			}
		}
		return protocol.MakeIntReply(n)
	default:
		return protocol.MakeSyntaxErrReply()
	}
}

func aropRange(arr *array.Array, start, end int64) [][]byte {
	if arr == nil {
		return nil
	}
	lo, hi := start, end
	if lo > hi {
		lo, hi = hi, lo
	}
	if lo < 0 {
		lo = 0
	}
	max := arr.Len()
	if hi < 0 || lo >= max {
		return nil
	}
	if hi >= max {
		hi = max - 1
	}
	return arr.Range(lo, hi)
}

type argparsePredicate struct {
	kind int // 0 exact, 1 match, 2 glob, 3 re
	val  string
}

// execARGrep ARGREP key start end <EXACT|MATCH|GLOB|RE> pattern [...]
// [AND|OR|LIMIT n|WITHVALUES|NOCASE]
func execARGrep(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'argrep' command")
	}
	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	length := int64(0)
	if arr != nil {
		length = arr.Len()
	}
	start, ok := parseARGrepBound(string(args[1]), true, length)
	end, ok2 := parseARGrepBound(string(args[2]), false, length)
	if !ok || !ok2 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	ascending := start <= end

	var preds []argparsePredicate
	withValues := false
	nocase := false
	useAnd := false
	limit := int64(-1)
	for i := 3; i < len(args); i++ {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "EXACT", "MATCH", "GLOB", "RE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'argrep' command")
			}
			kind := 0
			switch tok {
			case "MATCH":
				kind = 1
			case "GLOB":
				kind = 2
			case "RE":
				kind = 3
			}
			preds = append(preds, argparsePredicate{kind: kind, val: string(args[i+1])})
			i++
		case "AND":
			useAnd = true
		case "OR":
			useAnd = false
		case "LIMIT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'argrep' command")
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			limit = n
			i++
		case "WITHVALUES":
			withValues = true
		case "NOCASE":
			nocase = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	if len(preds) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'argrep' command")
	}
	lo, hi := start, end
	if lo > hi {
		lo, hi = hi, lo
	}
	if arr == nil || hi < 0 || lo >= length {
		return protocol.MakeEmptyMultiBulkReply()
	}
	if lo < 0 {
		lo = 0
	}
	if hi >= length {
		hi = length - 1
	}

	replies := make([]redis.Reply, 0)
	var collected int64
	appendMatch := func(i int64, v []byte) bool {
		if withValues {
			replies = append(replies, protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeIntReply(i),
				protocol.MakeBulkReply(v),
			}))
		} else {
			replies = append(replies, protocol.MakeIntReply(i))
		}
		collected++
		return limit > 0 && collected >= limit
	}
	if ascending {
		for i := lo; i <= hi; i++ {
			v, exists := arr.Get(i)
			if exists && argrepMatches(preds, v, useAnd, nocase) && appendMatch(i, v) {
				break
			}
		}
	} else {
		for i := hi; i >= lo; i-- {
			v, exists := arr.Get(i)
			if exists && argrepMatches(preds, v, useAnd, nocase) && appendMatch(i, v) {
				break
			}
		}
	}
	return protocol.MakeMultiRawReply(replies)
}

// parseARGrepBound parses "-" (first index) / "+" (last index) sentinels.
func parseARGrepBound(s string, start bool, length int64) (int64, bool) {
	switch s {
	case "-":
		return 0, true
	case "+":
		return length - 1, true
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func argrepMatches(preds []argparsePredicate, value []byte, useAnd, nocase bool) bool {
	if useAnd {
		for _, p := range preds {
			if !argrepMatchOne(p, value, nocase) {
				return false
			}
		}
		return true
	}
	for _, p := range preds {
		if argrepMatchOne(p, value, nocase) {
			return true
		}
	}
	return false
}

func argrepMatchOne(p argparsePredicate, value []byte, nocase bool) bool {
	s := string(value)
	pattern := p.val
	if nocase {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	switch p.kind {
	case 0:
		return s == pattern
	case 1:
		return strings.Contains(s, pattern)
	case 2:
		pat, err := wildcard.CompilePattern(pattern)
		return err == nil && pat.IsMatch(s)
	case 3:
		if nocase {
			pattern = "(?i)" + pattern
		}
		re, err := regexp.Compile(pattern)
		return err == nil && re.MatchString(s)
	}
	return false
}

// execARRing ARRING key size value [value ...] — ring-buffer insertion.
func execARRing(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arring' command")
	}
	size, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || size <= 0 || size > array.MaxIndex+1 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	arr, _, errReply := db.getOrInitArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	last, ok := arr.RingInsert(size, args[2:]...)
	if !ok {
		return protocol.MakeErrReply("ERR invalid ring size")
	}
	db.addAof(utils.ToCmdLine3("arring", args...))
	return protocol.MakeIntReply(last)
}

// execARScan ARSCAN key start end [LIMIT count] — index/value pairs of
// existing elements in the inclusive range, ascending or descending.
func execARScan(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 && len(args) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'arscan' command")
	}
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || start < 0 {
		return protocol.MakeErrReply("ERR invalid array index")
	}
	end, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil || end < 0 {
		return protocol.MakeErrReply("ERR invalid array index")
	}
	var limit int64
	if len(args) == 5 {
		if !strings.EqualFold(string(args[3]), "LIMIT") {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'arscan' command")
		}
		limit, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if limit <= 0 {
			return protocol.MakeErrReply("ERR LIMIT must be positive")
		}
	}

	arr, errReply := db.getAsArray(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if arr == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	entries := arr.Scan(start, end, limit)
	out := make([][]byte, 0, len(entries)*2)
	for _, e := range entries {
		out = append(out, []byte(strconv.FormatInt(e.Index, 10)), e.Value)
	}
	return protocol.MakeMultiBulkReply(out)
}

func init() {
	registerCommand("ARSet", execARSet, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ARMSet", execARMSet, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ARGet", execARGet, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARMGet", execARMGet, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARGetRange", execARGetRange, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARLen", execARLen, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARCount", execARCount, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARDel", execARDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ARDelRange", execARDelRange, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ARInsert", execARInsert, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARNext", execARNext, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ARSeek", execARSeek, writeFirstKey, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ARInfo", execARInfo, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARLastItems", execARLastItems, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("AROp", execAROp, readFirstKey, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARGrep", execARGrep, readFirstKey, nil, -6, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ARRing", execARRing, writeFirstKey, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ARScan", execARScan, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
