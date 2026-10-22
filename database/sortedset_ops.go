// Set-operation executors (ZUNION/ZINTER/ZDIFF stores, ZINTERCARD, ZMPop)
package database

import (
	"math"
	"sort"
	"strconv"
	"strings"

	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func execZUnion(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "UNION", false)
}

// execZUnionStore stores the union of multiple sorted sets
func execZUnionStore(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "UNION", true)
}

// execZInter computes the intersection of multiple sorted sets
func execZInter(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "INTER", false)
}

// execZInterStore stores the intersection of multiple sorted sets
func execZInterStore(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "INTER", true)
}

// execZDiff computes the difference of multiple sorted sets
func execZDiff(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "DIFF", false)
}

// execZDiffStore stores the difference of multiple sorted sets
func execZDiffStore(db *DB, args [][]byte) redis.Reply {
	return execZSetOperation(db, args, "DIFF", true)
}

// execZInterCard returns the intersection cardinality of multiple sorted sets.
// ZINTERCARD numkeys key [key ...] [LIMIT limit]
func execZInterCard(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zintercard' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		// Redis 8.x wording (differs from SINTERCARD).
		return protocol.MakeErrReply("ERR at least 1 input key is needed for 'zintercard' command")
	}
	if len(args) < 1+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zintercard' command")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[i+1])
	}

	// Parse LIMIT option (0 = unlimited).
	limit := 0
	if len(args) > 1+numKeys {
		if !strings.EqualFold(string(args[1+numKeys]), "LIMIT") {
			return protocol.MakeSyntaxErrReply()
		}
		if len(args) <= 2+numKeys {
			return protocol.MakeSyntaxErrReply()
		}
		n, perr := strconv.Atoi(string(args[2+numKeys]))
		if perr != nil || n < 0 {
			return protocol.MakeErrReply("ERR LIMIT can't be negative")
		}
		limit = n
	}

	// Load all zsets; an empty/missing zset yields cardinality 0.
	sets := make([]*SortedSet.SortedSet, 0, numKeys)
	for _, key := range keys {
		zset, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		if zset == nil {
			return protocol.MakeIntReply(0)
		}
		sets = append(sets, zset)
	}

	// Iterate the smallest zset and count members present in every other one.
	smallest := sets[0]
	for _, s := range sets[1:] {
		if s.Len() < smallest.Len() {
			smallest = s
		}
	}
	card := int64(0)
	for _, e := range smallest.RangeByRank(0, smallest.Len(), false) {
		inAll := true
		for _, s := range sets {
			if s == smallest {
				continue
			}
			if _, ok := s.Get(e.Member); !ok {
				inAll = false
				break
			}
		}
		if !inAll {
			continue
		}
		card++
		if limit > 0 && card >= int64(limit) {
			break // early stop at LIMIT
		}
	}
	return protocol.MakeIntReply(card)
}

// execZSetOperation performs set operations on sorted sets
func execZSetOperation(db *DB, args [][]byte, op string, store bool) redis.Reply {
	var numKeysIdx int
	if store {
		numKeysIdx = 1 // For ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE, first arg is dest key
	} else {
		numKeysIdx = 0 // For ZUNION/ZINTER/ZDIFF, first arg is numkeys
	}

	numKeys, err := strconv.Atoi(string(args[numKeysIdx]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		cmd := "z" + strings.ToLower(op)
		if store {
			cmd += "store"
		}
		return protocol.MakeErrReply("ERR at least 1 input key is needed for '" + cmd + "' command")
	}

	if len(args) < numKeysIdx+1+numKeys {
		return protocol.MakeErrReply("ERR syntax error")
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[numKeysIdx+1+i])
	}

	// Parse options
	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}
	aggregate := "SUM"
	withScores := false

	idx := numKeysIdx + 1 + numKeys
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx]))
		switch arg {
		case "WEIGHTS":
			if idx+numKeys >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			for i := 0; i < numKeys; i++ {
				w, err := strconv.ParseFloat(string(args[idx+1+i]), 64)
				if err != nil || math.IsNaN(w) {
					return protocol.MakeErrReply("ERR weight value is not a float")
				}
				weights[i] = w
			}
			idx += 1 + numKeys
		case "AGGREGATE":
			if idx+1 >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[idx+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" && aggregate != "COUNT" {
				return protocol.MakeErrReply("ERR syntax error")
			}
			// AGGREGATE COUNT (Redis 8.8) applies to UNION/INTER only; ZDIFF
			// has no AGGREGATE option in Redis.
			if aggregate == "COUNT" && op == "DIFF" {
				return protocol.MakeErrReply("ERR syntax error")
			}
			idx += 2
		case "WITHSCORES":
			withScores = true
			idx++
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	// Get all sorted sets
	sets := make([]*SortedSet.SortedSet, 0, numKeys)
	for _, key := range keys {
		sortedSet, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		sets = append(sets, sortedSet)
	}

	// Perform operation
	result := make(map[string]float64)

	switch op {
	case "UNION":
		for i, set := range sets {
			if set == nil {
				continue
			}
			members := set.RangeByRank(0, set.Len(), false)
			for _, m := range members {
				score := m.Score * weights[i]
				if aggregate == "COUNT" {
					// Original scores are ignored; each input set containing
					// the member contributes its weight (default 1).
					score = weights[i]
				}
				if existing, ok := result[m.Member]; ok {
					switch aggregate {
					case "SUM", "COUNT":
						result[m.Member] = existing + score
					case "MIN":
						if score < existing {
							result[m.Member] = score
						}
					case "MAX":
						if score > existing {
							result[m.Member] = score
						}
					}
				} else {
					result[m.Member] = score
				}
			}
		}

	case "INTER":
		// Find intersection; apply WEIGHTS then AGGREGATE (SUM/MIN/MAX)
		if len(sets) > 0 && sets[0] != nil {
			members := sets[0].RangeByRank(0, sets[0].Len(), false)
			for _, m := range members {
				scores := make([]float64, len(sets))
				scores[0] = m.Score * weights[0]
				if aggregate == "COUNT" {
					scores[0] = weights[0]
				}
				inAll := true
				for i := 1; i < len(sets); i++ {
					if sets[i] == nil {
						inAll = false
						break
					}
					elem, ok := sets[i].Get(m.Member)
					if !ok {
						inAll = false
						break
					}
					scores[i] = elem.Score * weights[i]
					if aggregate == "COUNT" {
						scores[i] = weights[i]
					}
				}
				if !inAll {
					continue
				}
				switch aggregate {
				case "MIN":
					min := scores[0]
					for _, s := range scores[1:] {
						if s < min {
							min = s
						}
					}
					result[m.Member] = min
				case "MAX":
					max := scores[0]
					for _, s := range scores[1:] {
						if s > max {
							max = s
						}
					}
					result[m.Member] = max
				default: // SUM and COUNT (weights summed, scores ignored)
					sum := 0.0
					for _, s := range scores {
						sum += s
					}
					result[m.Member] = sum
				}
			}
		}

	case "DIFF":
		// Find difference (first set minus others)
		if len(sets) > 0 && sets[0] != nil {
			members := sets[0].RangeByRank(0, sets[0].Len(), false)
			for _, m := range members {
				score := m.Score * weights[0]
				inOthers := false
				for i := 1; i < len(sets); i++ {
					if sets[i] != nil {
						if _, ok := sets[i].Get(m.Member); ok {
							inOthers = true
							break
						}
					}
				}
				if !inOthers {
					result[m.Member] = score
				}
			}
		}
	}

	if store {
		destKey := string(args[0]) // For store commands, first arg is dest key
		newSet := SortedSet.Make()
		for member, score := range result {
			newSet.Add(member, score)
		}
		db.PutEntity(destKey, &database.DataEntity{Data: newSet})
		db.addAof(utils.ToCmdLine3(strings.ToLower(op)+"store", args...))
		return protocol.MakeIntReply(int64(newSet.Len()))
	}

	// Return results sorted by score
	type memberScore struct {
		member string
		score  float64
	}

	sortedResult := make([]memberScore, 0, len(result))
	for member, score := range result {
		sortedResult = append(sortedResult, memberScore{member, score})
	}

	// Sort by score ascending
	sort.Slice(sortedResult, func(i, j int) bool {
		if sortedResult[i].score != sortedResult[j].score {
			return sortedResult[i].score < sortedResult[j].score
		}
		return sortedResult[i].member < sortedResult[j].member
	})

	if withScores {
		members := make([]string, len(sortedResult))
		scores := make([]float64, len(sortedResult))
		for i, ms := range sortedResult {
			members[i] = ms.member
			scores[i] = ms.score
		}
		return protocol.MakeScorePairsReply(members, scores, true)
	}

	// Members only → Set in RESP3 (unordered); RESP2 array via SetReply.ToBytes.
	out := make([]string, len(sortedResult))
	for i, ms := range sortedResult {
		out[i] = ms.member
	}
	return stringsToSetReply(out)
}

// execZMPop removes and returns elements from multiple sorted sets
// ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
func execZMPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zmpop' command")
	}

	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		return protocol.MakeErrReply("ERR numkeys should be greater than 0")
	}

	if len(args) < 1+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'zmpop' command")
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}

	// Parse options
	minMax := "MIN"
	count := 1

	idx := 1 + numKeys
	for idx < len(args) {
		arg := strings.ToUpper(string(args[idx]))
		switch arg {
		case "MIN", "MAX":
			minMax = arg
			idx++
		case "COUNT":
			if idx+1 >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			count, err = strconv.Atoi(string(args[idx+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			if count <= 0 {
				return protocol.MakeErrReply("ERR count should be greater than 0")
			}
			idx += 2
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	// Try each key
	for _, key := range keys {
		sortedSet, errReply := db.getAsSortedSet(key)
		if errReply != nil {
			return errReply
		}
		if sortedSet == nil || sortedSet.Len() == 0 {
			continue
		}

		// Pop elements
		var removed []*SortedSet.Element
		if minMax == "MIN" {
			removed = sortedSet.PopMin(count)
		} else {
			removed = sortedSet.PopMax(count)
		}

		if len(removed) == 0 {
			continue
		}

		// Build reply: [key, [[member, score], ...]] as nested MultiRawReply
		elements := make([]redis.Reply, 0, len(removed))
		for _, elem := range removed {
			elements = append(elements, protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeBulkReply([]byte(elem.Member)),
				protocol.MakeDoubleReply(elem.Score),
			}))
		}
		if sortedSet.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLine3("zmpop", args...))
		return protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeBulkReply([]byte(key)),
			protocol.MakeMultiRawReply(elements),
		})
	}

	return protocol.MakeNullBulkReply()
}

// execBZMPop is the blocking version of ZMPop
// BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
func execBZMPop(db *DB, args [][]byte) redis.Reply {
	// Minimum without COUNT: timeout numkeys key MIN|MAX → 4 args.
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bzmpop' command")
	}
	timeout, errReply := parseBlockTimeout(args[0])
	if errReply != nil {
		return errReply
	}

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		return protocol.MakeErrReply("ERR numkeys should be greater than 0")
	}
	// Need numkeys keys + MIN|MAX after timeout/numkeys.
	if len(args) < 3+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'bzmpop' command")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	for {
		db.RWLocks(keys, nil)
		result := execZMPop(db, args[1:])
		db.RWUnLocks(keys, nil)
		if _, ok := result.(*protocol.NullBulkReply); !ok {
			return result
		}
		w := registerZSetWaiter(keys)
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterZSetWaiter(w)
		if r := replyAfterWait(w, signaled); r != nil {
			return r
		}
	}
}

func prepareZMPop(args [][]byte) ([]string, []string) {
	if len(args) < 1 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 1 || len(args) < 1+numKeys {
		return nil, nil
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	return keys, nil
}
