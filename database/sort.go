package database

import (
	"math"
	"sort"
	"strconv"
	"strings"

	List "github.com/linkerlin/godis/datastruct/list"
	Set "github.com/linkerlin/godis/datastruct/set"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execSort sorts the elements in a list, set or sorted set
// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
func execSort(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'sort' command")
	}

	key := string(args[0])
	byPattern := ""
	offset := 0
	count := -1
	getPatterns := make([]string, 0)
	alpha := false
	desc := false
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
			desc = false
		case "DESC":
			desc = true
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

	entity, exists := db.GetEntity(key)
	if !exists {
		if storeDest != "" {
			db.Remove(storeDest)
			return protocol.MakeIntReply(0)
		}
		return protocol.MakeEmptyMultiBulkReply()
	}

	var elements []sortElement
	switch val := entity.Data.(type) {
	case List.List:
		val.ForEach(func(i int, v interface{}) bool {
			b := v.([]byte)
			elements = append(elements, sortElement{value: b, member: string(b)})
			return true
		})
	case *Set.Set:
		val.ForEach(func(member string) bool {
			elements = append(elements, sortElement{value: []byte(member), member: member})
			return true
		})
	case *SortedSet.SortedSet:
		members := val.RangeByRank(0, val.Len(), false)
		for _, elem := range members {
			elements = append(elements, sortElement{
				value:  []byte(elem.Member),
				member: elem.Member,
				score:  elem.Score,
			})
		}
	default:
		return &protocol.WrongTypeErrReply{}
	}

	noSort := strings.EqualFold(byPattern, "nosort")
	if !noSort {
		for i := range elements {
			if byPattern != "" {
				lookup := strings.Replace(byPattern, "*", elements[i].member, 1)
				raw, errReply := db.getAsString(lookup)
				if errReply != nil {
					return errReply
				}
				if alpha {
					elements[i].cmpStr = string(raw)
				} else {
					elements[i].score = parseSortWeight(raw)
				}
			} else if alpha {
				elements[i].cmpStr = elements[i].member
			} else if _, isZ := entity.Data.(*SortedSet.SortedSet); !isZ {
				elements[i].score = parseSortWeight(elements[i].value)
			}
		}

		sort.SliceStable(elements, func(i, j int) bool {
			cmp := 0
			if alpha {
				switch {
				case elements[i].cmpStr < elements[j].cmpStr:
					cmp = -1
				case elements[i].cmpStr > elements[j].cmpStr:
					cmp = 1
				case elements[i].member < elements[j].member:
					cmp = -1
				case elements[i].member > elements[j].member:
					cmp = 1
				}
			} else {
				switch {
				case elements[i].score < elements[j].score:
					cmp = -1
				case elements[i].score > elements[j].score:
					cmp = 1
				case elements[i].member < elements[j].member:
					cmp = -1
				case elements[i].member > elements[j].member:
					cmp = 1
				}
			}
			if desc {
				return cmp > 0
			}
			return cmp < 0
		})
	}

	if offset < 0 {
		offset = 0
	}
	if count < 0 {
		count = len(elements)
	}
	end := offset + count
	if offset >= len(elements) {
		elements = nil
	} else {
		if end > len(elements) {
			end = len(elements)
		}
		elements = elements[offset:end]
	}

	var result [][]byte
	for _, elem := range elements {
		if len(getPatterns) == 0 {
			result = append(result, elem.value)
			continue
		}
		for _, pattern := range getPatterns {
			if pattern == "#" {
				result = append(result, elem.value)
				continue
			}
			getKey := strings.Replace(pattern, "*", elem.member, 1)
			raw, errReply := db.getAsString(getKey)
			if errReply != nil {
				return errReply
			}
			if raw == nil {
				result = append(result, nil) // null bulk
			} else {
				result = append(result, raw)
			}
		}
	}

	if storeDest != "" {
		if len(result) == 0 {
			db.Remove(storeDest)
			db.addAof(utils.ToCmdLine3("sort", args...))
			return protocol.MakeIntReply(0)
		}
		list := List.NewQuickList()
		for _, v := range result {
			if v == nil {
				list.Add([]byte{})
			} else {
				list.Add(v)
			}
		}
		db.PutEntity(storeDest, &database.DataEntity{Data: list})
		db.addAof(utils.ToCmdLine3("sort", args...))
		return protocol.MakeIntReply(int64(len(result)))
	}

	return protocol.MakeMultiBulkReply(result)
}

func parseSortWeight(raw []byte) float64 {
	if len(raw) == 0 {
		return 0
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return 0
	}
	if math.IsNaN(f) {
		return 0
	}
	return f
}

type sortElement struct {
	value  []byte
	member string
	score  float64
	cmpStr string
}

func prepareSort(args [][]byte) ([]string, []string) {
	if len(args) < 1 {
		return nil, nil
	}
	write := []string{string(args[0])}
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(string(args[i]), "STORE") && i+1 < len(args) {
			write = append(write, string(args[i+1]))
			i++
		}
	}
	return write, nil
}

func init() {
	registerCommand("Sort", execSort, prepareSort, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}
