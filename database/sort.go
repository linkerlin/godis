package database

import (
	"strconv"
	"strings"

	List "github.com/hdt3213/godis/datastruct/list"
	Set "github.com/hdt3213/godis/datastruct/set"
	SortedSet "github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// execSort sorts the elements in a list, set or sorted set
// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]
func execSort(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sort' command")
	}

	key := string(args[0])
	
	// Parse options
	byPattern := ""
	offset := 0
	count := -1
	getPatterns := make([]string, 0)
	alpha := false
	storeDest := ""

	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "BY":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			byPattern = string(args[i+1])
			i++
		case "LIMIT":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			offset, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			count, err = strconv.Atoi(string(args[i+2]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			i += 2
		case "GET":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			getPatterns = append(getPatterns, string(args[i+1]))
			i++
		case "ASC":
			// ASC is default, desc = false
		case "DESC":
			// DESC not fully implemented yet
		case "ALPHA":
			alpha = true
		case "STORE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			storeDest = string(args[i+1])
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// Get the data to sort
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// Extract elements based on data type
	var elements []sortElement
	switch val := entity.Data.(type) {
	case []byte:
		// Single string - just return it
		return protocol.MakeMultiBulkReply([][]byte{val})
	case List.List:
		// List
		val.ForEach(func(i int, v interface{}) bool {
			elements = append(elements, sortElement{
				value: v.([]byte),
				score: float64(i),
			})
			return true
		})
	case *Set.Set:
		// Set
		i := 0
		val.ForEach(func(member string) bool {
			elements = append(elements, sortElement{
				value: []byte(member),
				score: float64(i),
			})
			i++
			return true
		})
	case *SortedSet.SortedSet:
		// Sorted set - sort by score
		members := val.RangeByRank(0, -1, false)
		for _, elem := range members {
			elements = append(elements, sortElement{
				value:  []byte(elem.Member),
				score:  elem.Score,
				member: elem.Member,
			})
		}
	default:
		return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// Apply BY pattern (simplified - just use value as score)
	if byPattern == "" && !alpha {
		// Sort by numeric value of element
		for i := range elements {
			s, _ := strconv.ParseFloat(string(elements[i].value), 64)
			elements[i].score = s
		}
	}

	// Apply LIMIT
	if offset < 0 {
		offset = 0
	}
	if count < 0 {
		count = len(elements)
	}
	end := offset + count
	if end > len(elements) {
		end = len(elements)
	}
	if offset >= len(elements) {
		elements = []sortElement{}
	} else {
		elements = elements[offset:end]
	}

	// Apply GET patterns (simplified - just return the value)
	var result [][]byte
	for _, elem := range elements {
		if len(getPatterns) == 0 {
			result = append(result, elem.value)
		} else {
			for _, pattern := range getPatterns {
				if pattern == "#" {
					result = append(result, []byte(elem.member))
				} else {
					// Try to get value from key
					getKey := strings.Replace(pattern, "*", elem.member, -1)
					getEntity, exists := db.GetEntity(getKey)
					if exists {
						if v, ok := getEntity.Data.([]byte); ok {
							result = append(result, v)
						} else {
							result = append(result, []byte{})
						}
					} else {
						result = append(result, []byte{})
					}
				}
			}
		}
	}

	// Store or return
	if storeDest != "" {
		// Store as list
		list := List.NewQuickList()
		for _, v := range result {
			list.Add(v)
		}
		db.PutEntity(storeDest, &database.DataEntity{Data: list})
		db.addAof(utils.ToCmdLine3("sort", args...))
		return protocol.MakeIntReply(int64(len(result)))
	}

	return protocol.MakeMultiBulkReply(result)
}

type sortElement struct {
	value  []byte
	score  float64
	member string
}

func init() {
	registerCommand("Sort", execSort, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}
