package database

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/list"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// UnWatch clears all watched keys for the connection.
func UnWatch(conn redis.Connection) redis.Reply {
	watching := conn.GetWatching()
	for k := range watching {
		delete(watching, k)
	}
	return protocol.MakeOkReply()
}

// execSMove moves member from source to destination set.
// SMOVE source destination member
func execSMove(db *DB, args [][]byte) redis.Reply {
	srcKey := string(args[0])
	dstKey := string(args[1])
	member := string(args[2])

	src, errReply := db.getAsSet(srcKey)
	if errReply != nil {
		return errReply
	}
	if src == nil || !src.Has(member) {
		return protocol.MakeIntReply(0)
	}

	dst, _, errReply := db.getOrInitSet(dstKey)
	if errReply != nil {
		return errReply
	}

	src.Remove(member)
	if src.Len() == 0 {
		db.Remove(srcKey)
	}
	dst.Add(member)
	db.addAof(utils.ToCmdLine3("smove", args...))
	return protocol.MakeIntReply(1)
}

func prepareSMove(args [][]byte) ([]string, []string) {
	return []string{string(args[0]), string(args[1])}, nil
}

// execLPos finds positions of element in a list.
// LPOS key element [RANK rank] [COUNT num] [MAXLEN len]
func execLPos(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lpos' command")
	}
	key := string(args[0])
	element := string(args[1])
	rank := int64(1)
	var count int64 = -1 // -1 = single reply mode
	var maxlen int64 = -1

	for i := 2; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		if i+1 >= len(args) {
			return protocol.MakeSyntaxErrReply()
		}
		n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		switch opt {
		case "RANK":
			if n == 0 {
				return protocol.MakeErrReply("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
			}
			rank = n
			i += 2
		case "COUNT":
			if n < 0 {
				return protocol.MakeErrReply("ERR COUNT can't be negative")
			}
			count = n
			i += 2
		case "MAXLEN":
			if n < 0 {
				return protocol.MakeErrReply("ERR MAXLEN can't be negative")
			}
			maxlen = n
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	lst, errReply := db.getAsList(key)
	if errReply != nil {
		return errReply
	}
	if lst == nil {
		if count >= 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		return protocol.MakeNullBulkReply()
	}

	indices := collectLPos(lst, element, rank, count, maxlen)
	if count >= 0 {
		out := make([][]byte, len(indices))
		for i, idx := range indices {
			out[i] = []byte(strconv.FormatInt(idx, 10))
		}
		return protocol.MakeMultiBulkReply(out)
	}
	if len(indices) == 0 {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(indices[0])
}

func collectLPos(lst list.List, element string, rank, count, maxlen int64) []int64 {
	var matches []int64
	visited := int64(0)
	lst.ForEach(func(i int, v interface{}) bool {
		if maxlen >= 0 && visited >= maxlen {
			return false
		}
		visited++
		bytes, _ := v.([]byte)
		if string(bytes) == element {
			matches = append(matches, int64(i))
		}
		return true
	})
	if len(matches) == 0 {
		return nil
	}

	var selected []int64
	if rank > 0 {
		idx := int(rank - 1)
		if idx >= len(matches) {
			return nil
		}
		selected = matches[idx:]
	} else {
		idx := len(matches) + int(rank)
		if idx < 0 || idx >= len(matches) {
			return nil
		}
		for i := idx; i >= 0; i-- {
			selected = append(selected, matches[i])
		}
	}

	if count == 0 {
		return selected
	}
	if count < 0 {
		count = 1
	}
	if int64(len(selected)) > count {
		selected = selected[:count]
	}
	return selected
}

// execBRPopLPush is BRPOPLPUSH source destination timeout (blocking RPOPLPUSH).
func execBRPopLPush(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'brpoplpush' command")
	}
	timeoutSec, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil || timeoutSec < 0 {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))
	source := string(args[0])
	lockKeys := []string{source, string(args[1])}
	for {
		db.RWLocks(lockKeys, nil)
		result := execRPopLPush(db, args[:2])
		db.RWUnLocks(lockKeys, nil)
		if _, ok := result.(*protocol.NullBulkReply); !ok {
			return result
		}
		w := registerListWaiter([]string{source})
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterListWaiter(w)
		if r := replyAfterWait(w, signaled); r != nil {
			return r
		}
	}
}

// execZRangeStore stores a range of a sorted set into another key.
// ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
func execZRangeStore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangestore' command")
	}
	dst := string(args[0])
	src := string(args[1])
	byScore, byLex, rev := false, false, false
	var limitOffset, limitCount int64 = 0, -1
	i := 4
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "BYSCORE":
			byScore = true
			i++
		case "BYLEX":
			byLex = true
			i++
		case "REV":
			rev = true
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			limitOffset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			limitCount, err = strconv.ParseInt(string(args[i+2]), 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			i += 3
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	if byScore && byLex {
		return protocol.MakeSyntaxErrReply()
	}

	srcSet, errReply := db.getAsSortedSet(src)
	if errReply != nil {
		return errReply
	}
	db.Remove(dst)
	if srcSet == nil || srcSet.Len() == 0 {
		return protocol.MakeIntReply(0)
	}

	var elements []*SortedSet.Element
	if byLex {
		minBorder, err := SortedSet.ParseLexBorder(string(args[2]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		maxBorder, err := SortedSet.ParseLexBorder(string(args[3]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		if rev {
			elements = srcSet.Range(maxBorder, minBorder, limitOffset, limitCount, true)
		} else {
			elements = srcSet.Range(minBorder, maxBorder, limitOffset, limitCount, false)
		}
	} else if byScore {
		minBorder, err := SortedSet.ParseScoreBorder(string(args[2]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		maxBorder, err := SortedSet.ParseScoreBorder(string(args[3]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		if rev {
			elements = srcSet.Range(maxBorder, minBorder, limitOffset, limitCount, true)
		} else {
			elements = srcSet.Range(minBorder, maxBorder, limitOffset, limitCount, false)
		}
	} else {
		start, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		stop, err := strconv.ParseInt(string(args[3]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		size := srcSet.Len()
		if start < -size {
			start = 0
		} else if start < 0 {
			start = size + start
		}
		if stop < -size {
			stop = 0
		} else if stop < 0 {
			stop = size + stop + 1
		} else if stop < size {
			stop = stop + 1
		} else {
			stop = size
		}
		if start >= size || stop <= start {
			return protocol.MakeIntReply(0)
		}
		elements = srcSet.RangeByRank(start, stop, rev)
		if limitOffset > 0 || limitCount >= 0 {
			if limitOffset >= int64(len(elements)) {
				elements = nil
			} else {
				elements = elements[limitOffset:]
				if limitCount >= 0 && int64(len(elements)) > limitCount {
					elements = elements[:limitCount]
				}
			}
		}
	}

	if len(elements) == 0 {
		return protocol.MakeIntReply(0)
	}
	dstSet := SortedSet.Make()
	for _, e := range elements {
		dstSet.Add(e.Member, e.Score)
	}
	db.PutEntity(dst, &database.DataEntity{Data: dstSet})
	db.addAof(utils.ToCmdLine3("zrangestore", args...))
	return protocol.MakeIntReply(int64(len(elements)))
}

func prepareZRangeStore(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return writeFirstKey(args)
	}
	return []string{string(args[0]), string(args[1])}, nil
}

// execZRandMember returns one or more random members from a sorted set.
// ZRANDMEMBER key [count [WITHSCORES]]
func execZRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrandmember' command")
	}
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil || sortedSet.Len() == 0 {
		if len(args) >= 2 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		return protocol.MakeNullBulkReply()
	}

	withScores := false
	countSpecified := false
	count := int64(1)
	if len(args) >= 2 {
		countSpecified = true
		n, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		count = n
	}
	if len(args) == 3 {
		if strings.ToUpper(string(args[2])) != "WITHSCORES" {
			return protocol.MakeSyntaxErrReply()
		}
		withScores = true
	}

	all := sortedSet.RangeByRank(0, sortedSet.Len(), false)
	if !countSpecified {
		e := all[rand.Intn(len(all))]
		return protocol.MakeBulkReply([]byte(e.Member))
	}

	allowDup := count < 0
	if allowDup {
		count = -count
	}
	if count == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	var picked []*SortedSet.Element
	if allowDup {
		picked = make([]*SortedSet.Element, count)
		for i := int64(0); i < count; i++ {
			picked[i] = all[rand.Intn(len(all))]
		}
	} else {
		if count > int64(len(all)) {
			count = int64(len(all))
		}
		perm := rand.Perm(len(all))
		picked = make([]*SortedSet.Element, count)
		for i := int64(0); i < count; i++ {
			picked[i] = all[perm[i]]
		}
	}

	if withScores {
		out := make([][]byte, 0, len(picked)*2)
		for _, e := range picked {
			out = append(out, []byte(e.Member))
			out = append(out, []byte(strconv.FormatFloat(e.Score, 'f', -1, 64)))
		}
		return protocol.MakeMultiBulkReply(out)
	}
	out := make([][]byte, len(picked))
	for i, e := range picked {
		out[i] = []byte(e.Member)
	}
	return protocol.MakeMultiBulkReply(out)
}

// execHExpireTime returns absolute Unix expire time (seconds) of hash fields.
// HEXPIRETIME key FIELDS numfields field [field ...]
func execHExpireTime(db *DB, args [][]byte) redis.Reply {
	return execHExpireTimeFamily(db, args, false)
}

// execHPExpireTime returns absolute Unix expire time (milliseconds) of hash fields.
func execHPExpireTime(db *DB, args [][]byte) redis.Reply {
	return execHExpireTimeFamily(db, args, true)
}

func execHExpireTimeFamily(db *DB, args [][]byte, millis bool) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	key := string(args[0])
	if strings.ToUpper(string(args[1])) != "FIELDS" {
		return protocol.MakeSyntaxErrReply()
	}
	n, err := strconv.Atoi(string(args[2]))
	if err != nil || n < 1 {
		return protocol.MakeErrReply("ERR Number of fields can't be negative or zero")
	}
	if len(args) != 3+n {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	ed, errReply := db.getAsExpireDict(key)
	if errReply != nil {
		return errReply
	}
	result := make([]redis.Reply, n)
	for i := 0; i < n; i++ {
		field := string(args[3+i])
		if ed == nil {
			result[i] = protocol.MakeIntReply(-2)
			continue
		}
		if _, exists := ed.Get(field); !exists {
			result[i] = protocol.MakeIntReply(-2)
			continue
		}
		exp, ok := ed.GetExpireTime(field)
		if !ok {
			result[i] = protocol.MakeIntReply(-1)
			continue
		}
		if millis {
			result[i] = protocol.MakeIntReply(exp.UnixNano() / int64(time.Millisecond))
		} else {
			result[i] = protocol.MakeIntReply(exp.Unix())
		}
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerCommand("SMove", execSMove, prepareSMove, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 2, 1)
	registerCommand("LPos", execLPos, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("BRPopLPush", execBRPopLPush, nil, nil, 4, flagSpecial).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagBlocking}, 1, 2, 1)
	registerCommand("ZRangeStore", execZRangeStore, prepareZRangeStore, rollbackFirstKey, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("ZRandMember", execZRandMember, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("HExpireTime", execHExpireTime, readFirstKey, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("HPExpireTime", execHPExpireTime, readFirstKey, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
}
