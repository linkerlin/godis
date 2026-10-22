package database

import (
	"math"
	"strconv"
	"strings"

	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func (db *DB) getAsSortedSet(key string) (*SortedSet.SortedSet, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	sortedSet, ok := entity.Data.(*SortedSet.SortedSet)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return sortedSet, nil
}

func (db *DB) getOrInitSortedSet(key string) (sortedSet *SortedSet.SortedSet, inited bool, errReply protocol.ErrorReply) {
	sortedSet, errReply = db.getAsSortedSet(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited = false
	if sortedSet == nil {
		sortedSet = SortedSet.Make()
		db.PutEntity(key, &database.DataEntity{
			Data: sortedSet,
		})
		inited = true
	}
	return sortedSet, inited, nil
}

// execZAdd adds member into sorted set
// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
func execZAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zadd' command")
	}
	key := string(args[0])
	nx, xx, gt, lt, ch, incr := false, false, false, false, false, false
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX":
			nx = true
			i++
		case "XX":
			xx = true
			i++
		case "GT":
			gt = true
			i++
		case "LT":
			lt = true
			i++
		case "CH":
			ch = true
			i++
		case "INCR":
			incr = true
			i++
		default:
			goto parsePairs
		}
	}
parsePairs:
	if nx && xx {
		return protocol.MakeErrReply("ERR XX and NX options at the same time are not compatible")
	}
	if gt && lt || nx && (gt || lt) {
		return protocol.MakeErrReply("ERR GT, LT, and/or NX options at the same time are not compatible")
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return protocol.MakeSyntaxErrReply()
	}
	if incr && len(rest) != 2 {
		return protocol.MakeErrReply("ERR INCR option supports a single increment-element pair")
	}

	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	if incr {
		scoreDelta, err := strconv.ParseFloat(string(rest[0]), 64)
		if err != nil || math.IsNaN(scoreDelta) || math.IsInf(scoreDelta, 0) {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		member := string(rest[1])
		cur, exists := sortedSet.Get(member)
		if nx && exists {
			return protocol.MakeNullBulkReply()
		}
		if xx && !exists {
			return protocol.MakeNullBulkReply()
		}
		var newScore float64
		if exists {
			newScore = cur.Score + scoreDelta
			if gt && !(newScore > cur.Score) {
				return protocol.MakeNullBulkReply()
			}
			if lt && !(newScore < cur.Score) {
				return protocol.MakeNullBulkReply()
			}
		} else {
			newScore = scoreDelta
		}
		if math.IsNaN(newScore) || math.IsInf(newScore, 0) {
			return protocol.MakeErrReply("ERR resulting score is not a number (NaN)")
		}
		sortedSet.Add(member, newScore)
		db.addAof(utils.ToCmdLine3("zadd", args...))
		notifyKeyspaceEvent(db, "zadd", key)
		signalZSetWaiters(key)
		return protocol.MakeDoubleReply(newScore)
	}

	added, changed := 0, 0
	for j := 0; j < len(rest); j += 2 {
		score, err := strconv.ParseFloat(string(rest[j]), 64)
		if err != nil || math.IsNaN(score) {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		member := string(rest[j+1])
		cur, exists := sortedSet.Get(member)
		if nx && exists {
			continue
		}
		if xx && !exists {
			continue
		}
		if exists {
			if gt && !(score > cur.Score) {
				continue
			}
			if lt && !(score < cur.Score) {
				continue
			}
			if score != cur.Score {
				sortedSet.Add(member, score)
				changed++
			}
		} else {
			sortedSet.Add(member, score)
			added++
			changed++
		}
	}

	db.addAof(utils.ToCmdLine3("zadd", args...))
	if added+changed > 0 {
		notifyKeyspaceEvent(db, "zadd", key)
	}
	signalZSetWaiters(key)
	if ch {
		return protocol.MakeIntReply(int64(changed))
	}
	return protocol.MakeIntReply(int64(added))
}

func undoZAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX", "XX", "GT", "LT", "CH", "INCR":
			i++
		default:
			goto done
		}
	}
done:
	rest := args[i:]
	fields := make([]string, 0, len(rest)/2)
	for j := 1; j < len(rest); j += 2 {
		fields = append(fields, string(rest[j]))
	}
	return rollbackZSetFields(db, key, fields...)
}

// elementsToScorePairs builds a RESP2-flat / RESP3-(nested|flat) score-pairs reply.
func elementsToScorePairs(elements []*SortedSet.Element, nest bool) redis.Reply {
	if len(elements) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	members := make([]string, len(elements))
	scores := make([]float64, len(elements))
	for i, e := range elements {
		members[i] = e.Member
		scores[i] = e.Score
	}
	return protocol.MakeScorePairsReply(members, scores, nest)
}

// execZScore gets score of a member in sortedset.
// Returns DoubleReply so RESP3 connections get ,doubles while RESP2 still sees a bulk string.
func execZScore(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	element, exists := sortedSet.Get(member)
	if !exists {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeDoubleReply(element.Score)
}

// execZMScore gets scores of multiple members in sortedset
// ZMSCORE key member [member ...]
func execZMScore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zmscore' command")
	}

	key := string(args[0])
	members := args[1:]
	if reply := validateBulkBytesSlice(members); reply != nil {
		return reply
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}

	results := make([]redis.Reply, len(members))
	for i, member := range members {
		if sortedSet == nil {
			results[i] = protocol.MakeNullBulkReply()
			continue
		}
		element, exists := sortedSet.Get(string(member))
		if !exists {
			results[i] = protocol.MakeNullBulkReply()
		} else {
			results[i] = protocol.MakeDoubleReply(element.Score)
		}
	}

	return protocol.MakeMultiRawReply(results)
}

// execZRank gets index of a member in sortedset, ascending order, start from 0
// ZRANK key member [WITHSCORE]
func execZRank(db *DB, args [][]byte) redis.Reply {
	return execZRankFamily(db, args, false)
}

// execZRevRank gets index of a member in sortedset, descending order, start from 0
// ZREVRANK key member [WITHSCORE]
func execZRevRank(db *DB, args [][]byte) redis.Reply {
	return execZRankFamily(db, args, true)
}

func execZRankFamily(db *DB, args [][]byte, desc bool) redis.Reply {
	if len(args) != 2 && len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	withScore := false
	if len(args) == 3 {
		if strings.ToUpper(string(args[2])) != "WITHSCORE" {
			return protocol.MakeSyntaxErrReply()
		}
		withScore = true
	}
	key := string(args[0])
	member := string(args[1])

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	rank := sortedSet.GetRank(member, desc)
	if rank < 0 {
		return &protocol.NullBulkReply{}
	}
	if !withScore {
		return protocol.MakeIntReply(rank)
	}
	ele, _ := sortedSet.Get(member)
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeIntReply(rank),
		protocol.MakeDoubleReply(ele.Score),
	})
}

// execZCard gets number of members in sortedset
func execZCard(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(sortedSet.Len())
}

// execZRange gets members in range. Redis 6.2+ unified syntax:
// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
func execZRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrange' command")
	}
	key := string(args[0])
	byScore, byLex, rev, withScores := false, false, false, false
	var limitOffset, limitCount int64 = 0, -1
	i := 3
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
		case "WITHSCORES":
			withScores = true
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
	if byLex && withScores {
		return protocol.MakeErrReply("ERR syntax error, WITHSCORES not supported in combination with BYLEX")
	}

	// Redis: validate range args before miss / WRONGTYPE.
	if byLex {
		minBorder, err := SortedSet.ParseLexBorder(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		maxBorder, err := SortedSet.ParseLexBorder(string(args[2]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		sortedSet, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		if sortedSet == nil {
			return &protocol.EmptyMultiBulkReply{}
		}
		var elements []*SortedSet.Element
		if rev {
			elements = sortedSet.Range(maxBorder, minBorder, limitOffset, limitCount, true)
		} else {
			elements = sortedSet.Range(minBorder, maxBorder, limitOffset, limitCount, false)
		}
		result := make([][]byte, len(elements))
		for i, e := range elements {
			result[i] = []byte(e.Member)
		}
		return protocol.MakeMultiBulkReply(result)
	}
	if byScore {
		minBorder, err := SortedSet.ParseScoreBorder(string(args[1]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		maxBorder, err := SortedSet.ParseScoreBorder(string(args[2]))
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		sortedSet, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		if sortedSet == nil {
			return &protocol.EmptyMultiBulkReply{}
		}
		var elements []*SortedSet.Element
		if rev {
			elements = sortedSet.Range(maxBorder, minBorder, limitOffset, limitCount, true)
		} else {
			elements = sortedSet.Range(minBorder, maxBorder, limitOffset, limitCount, false)
		}
		if withScores {
			return elementsToScorePairs(elements, true)
		}
		result := make([][]byte, len(elements))
		for i, e := range elements {
			result[i] = []byte(e.Member)
		}
		return protocol.MakeMultiBulkReply(result)
	}

	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, rev)
}

// execZRevRange gets members in range, sort by score in descending order
func execZRevRange(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrange' command")
	}
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			return protocol.MakeErrReply("ERR syntax error")
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	return range0(db, key, start, stop, withScores, true)
}

func range0(db *DB, key string, start int64, stop int64, withScores bool, desc bool) redis.Reply {
	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return &protocol.EmptyMultiBulkReply{}
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	slice := sortedSet.RangeByRank(start, stop, desc)
	if withScores {
		return elementsToScorePairs(slice, true)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZCount gets number of members which score within given range
func execZCount(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	return protocol.MakeIntReply(sortedSet.RangeCount(min, max))
}

/*
 * param limit: limit < 0 means no limit
 */
func rangeByScore0(db *DB, key string, min SortedSet.Border, max SortedSet.Border, offset int64, limit int64, withScores bool, desc bool) redis.Reply {
	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.EmptyMultiBulkReply{}
	}

	slice := sortedSet.Range(min, max, offset, limit, desc)
	if withScores {
		return elementsToScorePairs(slice, true)
	}
	result := make([][]byte, len(slice))
	i := 0
	for _, element := range slice {
		result[i] = []byte(element.Member)
		i++
	}
	return protocol.MakeMultiBulkReply(result)
}

// execZRangeByScore gets members which score within given range, in ascending order
func execZRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, false)
}

// execZRevRangeByScore gets number of members which score within given range, in descending order
func execZRevRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	withScores := false
	var offset int64 = 0
	var limit int64 = -1
	if len(args) > 3 {
		for i := 3; i < len(args); {
			s := string(args[i])
			if strings.ToUpper(s) == "WITHSCORES" {
				withScores = true
				i++
			} else if strings.ToUpper(s) == "LIMIT" {
				if len(args) < i+3 {
					return protocol.MakeErrReply("ERR syntax error")
				}
				offset, err = strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				limit, err = strconv.ParseInt(string(args[i+2]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				i += 3
			} else {
				return protocol.MakeErrReply("ERR syntax error")
			}
		}
	}
	return rangeByScore0(db, key, min, max, offset, limit, withScores, true)
}

// execZRemRangeByScore removes members which score within given range
func execZRemRangeByScore(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebyscore' command")
	}
	key := string(args[0])

	min, err := SortedSet.ParseScoreBorder(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	max, err := SortedSet.ParseScoreBorder(string(args[2]))
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	removed := sortedSet.RemoveRange(min, max)
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyscore", args...))
	}
	return protocol.MakeIntReply(removed)
}

// execZRemRangeByRank removes members within given indexes
func execZRemRangeByRank(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	// get data
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	// compute index
	size := sortedSet.Len() // assert: size > 0
	if start < -1*size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return protocol.MakeIntReply(0)
	}
	if stop < -1*size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	if stop < start {
		stop = start
	}

	// assert: start in [0, size - 1], stop in [start, size]
	removed := sortedSet.RemoveByRank(start, stop)
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}
	if removed > 0 {
		db.addAof(utils.ToCmdLine3("zremrangebyrank", args...))
	}
	return protocol.MakeIntReply(removed)
}

func execZPopMin(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		// Redis 8.10: non-integer and negative share "must be positive".
		if err != nil || count64 < 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	// Redis: count 0 → empty array, no mutation (skiplist limit 0 means unlimited).
	if count == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	removed := sortedSet.PopMin(count)
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}
	if len(removed) > 0 {
		db.addAof(utils.ToCmdLine3("zpopmin", args...))
	}
	// Explicit COUNT → nested RESP3 pairs; bare ZPOPMIN → flat [member, score].
	return elementsToScorePairs(removed, len(args) > 1)
}

// execZRem removes given members
func execZRem(db *DB, args [][]byte) redis.Reply {
	// parse args
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	if reply := validateBulkBytesSlice(fieldArgs); reply != nil {
		return reply
	}

	// get entity
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	var deleted int64 = 0
	for _, field := range fields {
		if sortedSet.Remove(field) {
			deleted++
		}
	}
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("zrem", args...))
		notifyKeyspaceEvent(db, "zrem", key)
	}
	return protocol.MakeIntReply(deleted)
}

func undoZRem(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	fields := make([]string, len(args)-1)
	fieldArgs := args[1:]
	for i, v := range fieldArgs {
		fields[i] = string(v)
	}
	return rollbackZSetFields(db, key, fields...)
}

// execZIncrBy increments the score of a member
func execZIncrBy(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	if reply := validateBulkBytes(args[1]); reply != nil {
		return reply
	}
	if reply := validateBulkBytes(args[2]); reply != nil {
		return reply
	}
	rawDelta := string(args[1])
	field := string(args[2])
	delta, err := strconv.ParseFloat(rawDelta, 64)
	if err != nil || math.IsNaN(delta) {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}

	// get or init entity
	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	element, exists := sortedSet.Get(field)
	if !exists {
		sortedSet.Add(field, delta)
		db.addAof(utils.ToCmdLine3("zincrby", args...))
		signalZSetWaiters(key)
		return protocol.MakeDoubleReply(delta)
	}
	score := element.Score + delta
	if math.IsNaN(score) {
		return protocol.MakeErrReply("ERR resulting score is not a number (NaN)")
	}
	sortedSet.Add(field, score)
	db.addAof(utils.ToCmdLine3("zincrby", args...))
	signalZSetWaiters(key)
	return protocol.MakeDoubleReply(score)
}

func undoZIncr(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	field := string(args[2])
	return rollbackZSetFields(db, key, field)
}

func execZLexCount(db *DB, args [][]byte) redis.Reply {
	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	count := sortedSet.RangeCount(min, max)

	return protocol.MakeIntReply(count)
}

func execZRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n > 3 && strings.ToLower(string(args[3])) != "limit" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if n != 3 && n != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrangebylex' command")
	}

	key := string(args[0])
	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	offset := int64(0)
	limitCnt := int64(math.MaxInt64)
	if n > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if offset < 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limitCnt = count
		}
	}

	elements := sortedSet.Range(min, max, offset, limitCnt, false)
	result := make([][]byte, 0, len(elements))
	for _, ele := range elements {
		result = append(result, []byte(ele.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZRemRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zremrangebylex' command")
	}

	minEle, maxEle := string(args[1]), string(args[2])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeIntReply(0)
	}

	count := sortedSet.RemoveRange(min, max)
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}

	return protocol.MakeIntReply(count)
}

func execZRevRangeByLex(db *DB, args [][]byte) redis.Reply {
	n := len(args)
	if n > 3 && strings.ToLower(string(args[3])) != "limit" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	if n != 3 && n != 6 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zrevrangebylex' command")
	}

	key := string(args[0])
	// Redis ZREVRANGEBYLEX: args are max then min; validate lex borders before miss-key empty reply.
	minEle, maxEle := string(args[2]), string(args[1])
	min, err := SortedSet.ParseLexBorder(minEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	max, err := SortedSet.ParseLexBorder(maxEle)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	offset := int64(0)
	limitCnt := int64(math.MaxInt64)
	if n > 3 {
		var err error
		offset, err = strconv.ParseInt(string(args[4]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if offset < 0 {
			return protocol.MakeEmptyMultiBulkReply()
		}
		count, err := strconv.ParseInt(string(args[5]), 10, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if count >= 0 {
			limitCnt = count
		}
	}

	elements := sortedSet.Range(min, max, offset, limitCnt, true)
	result := make([][]byte, 0, len(elements))
	for _, ele := range elements {
		result = append(result, []byte(ele.Member))
	}
	if len(result) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(result)
}

func execZScan(db *DB, args [][]byte) redis.Reply {
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid cursor")
	}
	key := string(args[0])
	set, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return emptyScanReply()
	}
	count, pattern, optErr := parseTypeScanOptions(args, 2)
	if optErr != nil {
		return optErr
	}

	keysReply, nextCursor := set.ZSetScan(cursor, count, pattern)
	if nextCursor < 0 {
		return protocol.MakeErrReply("ERR invalid argument")
	}

	result := make([]redis.Reply, 2)
	result[0] = protocol.MakeBulkReply([]byte(strconv.FormatInt(int64(nextCursor), 10)))
	result[1] = protocol.MakeMultiBulkReply(keysReply)

	return protocol.MakeMultiRawReply(result)
}

// execZPopMax pops the members with highest scores from sorted set
func execZPopMax(db *DB, args [][]byte) redis.Reply {
	key := string(args[0])
	count := 1
	if len(args) > 1 {
		count64, err := strconv.ParseInt(string(args[1]), 10, 64)
		// Redis 8.10: non-integer and negative share "must be positive".
		if err != nil || count64 < 0 {
			return protocol.MakeErrReply("ERR value is out of range, must be positive")
		}
		count = int(count64)
	}

	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}
	// Redis: count 0 → empty array, no mutation.
	if count == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	removed := sortedSet.PopMax(count)
	if sortedSet.Len() == 0 {
		db.Remove(key)
	}
	if len(removed) > 0 {
		db.addAof(utils.ToCmdLine3("zpopmax", args...))
	}
	return elementsToScorePairs(removed, len(args) > 1)
}

// execBZPopMin BZPOPMIN key [key ...] timeout - 阻塞弹出最小分数成员
func execBZPopMin(db *DB, args [][]byte) redis.Reply {
	return execBlockingZPop(db, args, true)
}

// execBZPopMax BZPOPMAX key [key ...] timeout - 阻塞弹出最大分数成员
func execBZPopMax(db *DB, args [][]byte) redis.Reply {
	return execBlockingZPop(db, args, false)
}

func execBlockingZPop(db *DB, args [][]byte, min bool) redis.Reply {
	cmd := "bzpopmin"
	if !min {
		cmd = "bzpopmax"
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	timeout, errReply := parseBlockTimeout(args[len(args)-1])
	if errReply != nil {
		return errReply
	}
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}

	for {
		db.RWLocks(keys, nil)
		var reply redis.Reply
		for _, key := range keys {
			sortedSet, errReply := db.getAsSortedSet(key)
			if errReply != nil {
				db.RWUnLocks(keys, nil)
				return errReply
			}
			if sortedSet == nil || sortedSet.Len() == 0 {
				continue
			}
			var removed []*SortedSet.Element
			if min {
				removed = sortedSet.PopMin(1)
			} else {
				removed = sortedSet.PopMax(1)
			}
			if len(removed) == 0 {
				continue
			}
			if sortedSet.Len() == 0 {
				db.Remove(key)
			}
			db.addVersion(key)
			if min {
				db.addAof(utils.ToCmdLine3("zpopmin", []byte(key)))
			} else {
				db.addAof(utils.ToCmdLine3("zpopmax", []byte(key)))
			}
			reply = protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeBulkReply([]byte(key)),
				protocol.MakeBulkReply([]byte(removed[0].Member)),
				protocol.MakeDoubleReply(removed[0].Score),
			})
			break
		}
		db.RWUnLocks(keys, nil)
		if reply != nil {
			return reply
		}
		w := registerZSetWaiter(keys)
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterZSetWaiter(w)
		if r := replyAfterWait(w, signaled); r != nil {
			return r
		}
	}
}

func init() {
	registerCommand("ZAdd", execZAdd, writeFirstKey, undoZAdd, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZScore", execZScore, readFirstKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZMScore", execZMScore, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncr, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRank", execZRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCount", execZCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRevRank", execZRevRank, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZCard", execZCard, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByScore", execZRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRange", execZRevRange, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZPopMin", execZPopMin, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZPopMax", execZPopMax, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("ZRemRangeByScore", execZRemRangeByScore, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRemRangeByRank", execZRemRangeByRank, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZLexCount", execZLexCount, readFirstKey, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRangeByLex", execZRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZRemRangeByLex", execZRemRangeByLex, writeFirstKey, rollbackFirstKey, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("ZRevRangeByLex", execZRevRangeByLex, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("ZScan", execZScan, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("BZPopMin", execBZPopMin, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("BZPopMax", execBZPopMax, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("ZMPop", execZMPop, prepareZMPop, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -2, 1)
	registerCommand("BZMPop", execBZMPop, nil, nil, -5, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("ZUnion", execZUnion, prepareZCalculate, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, -1, 1)
	registerCommand("ZUnionStore", execZUnionStore, prepareZStoreCalculateStore, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("ZInter", execZInter, prepareZCalculate, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, -1, 1)
	registerCommand("ZInterStore", execZInterStore, prepareZStoreCalculateStore, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("ZDiff", execZDiff, prepareZCalculate, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 1, -1, 1)
	registerCommand("ZDiffStore", execZDiffStore, prepareZStoreCalculateStore, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("ZInterCard", execZInterCard, prepareZCalculate, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, -1, 1)
}

// execZUnion computes the union of multiple sorted sets
