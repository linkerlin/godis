package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// Redis 8 Vector Set command aliases. Legacy VS* names remain registered.
// Supported subset:
//
//	VADD key VALUES dim f1..fn ELE element
//	VSIM key VALUES dim f1..fn [COUNT n] [WITHSCORES]
//	VSIM key ELE element [COUNT n] [WITHSCORES]
//	VREM / VCARD / VDIM / VEMB / VINFO / VISMEMBER

func execVAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vadd' command")
	}
	key := string(args[0])
	var ele string
	var floats []float64
	i := 1
	for i < len(args) {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "VALUES":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			dim, err := strconv.Atoi(string(args[i+1]))
			if err != nil || dim <= 0 {
				return protocol.MakeErrReply("ERR invalid vector dimension")
			}
			if i+1+dim >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'vadd' command")
			}
			floats = make([]float64, dim)
			for d := 0; d < dim; d++ {
				f, err := strconv.ParseFloat(string(args[i+2+d]), 64)
				if err != nil {
					return protocol.MakeErrReply("ERR invalid vector component")
				}
				floats[d] = f
			}
			i += 2 + dim
		case "ELE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ele = string(args[i+1])
			i += 2
		case "FP32":
			return protocol.MakeErrReply("ERR FP32 binary form not supported; use VALUES")
		case "NX", "XX", "CAS", "NOQUANT", "Q8", "BIN", "TRUTH", "NOTHREAD":
			i++
		case "REDUCE", "EF", "M", "SETATTR":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}
	if ele == "" || floats == nil {
		return protocol.MakeErrReply("ERR VADD requires VALUES and ELE")
	}
	return execVSAdd(db, [][]byte{[]byte(key), []byte(ele), []byte(formatFloatsCSV(floats))})
}

func execVSim(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vsim' command")
	}
	key := string(args[0])
	count := 10
	withScores := false
	var floats []float64
	var ele string
	i := 1
	for i < len(args) {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "VALUES":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			dim, err := strconv.Atoi(string(args[i+1]))
			if err != nil || dim <= 0 {
				return protocol.MakeErrReply("ERR invalid vector dimension")
			}
			if i+1+dim >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'vsim' command")
			}
			floats = make([]float64, dim)
			for d := 0; d < dim; d++ {
				f, err := strconv.ParseFloat(string(args[i+2+d]), 64)
				if err != nil {
					return protocol.MakeErrReply("ERR invalid vector component")
				}
				floats[d] = f
			}
			i += 2 + dim
		case "ELE":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ele = string(args[i+1])
			i += 2
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n <= 0 {
				return protocol.MakeErrReply("ERR COUNT must be a positive integer")
			}
			count = n
			i += 2
		case "WITHSCORES":
			withScores = true
			i++
		case "EPSILON", "EF", "FILTER":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			i += 2
		case "TRUTH", "NOTHREAD":
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}

	var results []*vector.SearchResult
	if ele != "" {
		item, found := vs.Get(ele)
		if !found {
			return protocol.MakeEmptyMultiBulkReply()
		}
		results = vs.SearchWithMetric(item.Vector, count, vector.CosineSimilarity)
	} else if floats != nil {
		results = vs.SearchWithMetric(vector.NewVectorFromFloat64(floats), count, vector.CosineSimilarity)
	} else {
		return protocol.MakeErrReply("ERR VSIM requires VALUES or ELE")
	}
	return formatVSimResults(results, withScores)
}

func formatVSimResults(results []*vector.SearchResult, withScores bool) redis.Reply {
	if len(results) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	if withScores {
		out := make([][]byte, 0, len(results)*2)
		for _, r := range results {
			out = append(out, []byte(r.ID))
			out = append(out, []byte(strconv.FormatFloat(float64(r.Score), 'f', -1, 32)))
		}
		return protocol.MakeMultiBulkReply(out)
	}
	ids := make([][]byte, 0, len(results))
	for _, r := range results {
		ids = append(ids, []byte(r.ID))
	}
	return protocol.MakeMultiBulkReply(ids)
}

func execVRem(db *DB, args [][]byte) redis.Reply {
	return execVSDel(db, args)
}

func execVCard(db *DB, args [][]byte) redis.Reply {
	return execVSCard(db, args)
}

func execVDim(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vdim' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return protocol.MakeIntReply(int64(vs.Dimension()))
}

func execVEmb(db *DB, args [][]byte) redis.Reply {
	return execVSGet(db, args)
}

func execVInfo(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vinfo' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeErrReply("ERR key does not exist")
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("quant-type")),
		protocol.MakeBulkReply([]byte("f32")),
		protocol.MakeBulkReply([]byte("vector-dim")),
		protocol.MakeIntReply(int64(vs.Dimension())),
		protocol.MakeBulkReply([]byte("size")),
		protocol.MakeIntReply(int64(vs.Len())),
	})
}

func execVIsMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vismember' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeIntReply(0)
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	_, found := vs.Get(string(args[1]))
	if found {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

func formatFloatsCSV(fs []float64) string {
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strings.Join(parts, ",")
}

func init() {
	registerCommand("VAdd", execVAdd, prepareVSKey, nil, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("VSim", execVSim, prepareVSKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VRem", execVRem, prepareVSKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("VCard", execVCard, prepareVSKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("VDim", execVDim, prepareVSKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("VEmb", execVEmb, prepareVSKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VInfo", execVInfo, prepareVSKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VIsMember", execVIsMember, prepareVSKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
}
