package database

import (
	"encoding/json"
	"errors"
	"math/rand"
	"regexp"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
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
	withAttribs := false
	var filterExpr string
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
		case "WITHATTRIBS":
			withAttribs = true
			i++
		case "FILTER":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			filterExpr = string(args[i+1])
			i += 2
		case "EPSILON", "EF":
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

	searchLimit := count
	if filterExpr != "" {
		// Over-fetch then filter so COUNT still refers to matching hits.
		searchLimit = vs.Len()
		if searchLimit < count {
			searchLimit = count
		}
	}

	var results []*vector.SearchResult
	if ele != "" {
		item, found := vs.Get(ele)
		if !found {
			return protocol.MakeEmptyMultiBulkReply()
		}
		results = vs.SearchWithMetric(item.Vector, searchLimit, vector.CosineSimilarity)
	} else if floats != nil {
		results = vs.SearchWithMetric(vector.NewVectorFromFloat64(floats), searchLimit, vector.CosineSimilarity)
	} else {
		return protocol.MakeErrReply("ERR VSIM requires VALUES or ELE")
	}

	if filterExpr != "" {
		filtered := make([]*vector.SearchResult, 0, len(results))
		for _, r := range results {
			ok, err := matchVSimAttrFilter(r.Attributes, filterExpr)
			if err != nil {
				return protocol.MakeErrReply("ERR invalid FILTER expression")
			}
			if ok {
				filtered = append(filtered, r)
			}
		}
		results = filtered
		if len(results) > count {
			results = results[:count]
		}
	}
	return formatVSimResults(results, withScores, withAttribs)
}

var vsimFilterRE = regexp.MustCompile(`(?i)^\s*\.([A-Za-z_][\w]*)\s*(==|!=)\s*(.+?)\s*$`)

// matchVSimAttrFilter supports a minimal Redis-like FILTER: `.field == "value"` / `.field == 1` / `!=`.
func matchVSimAttrFilter(attrs, expr string) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}
	m := vsimFilterRE.FindStringSubmatch(expr)
	if m == nil {
		return false, errInvalidVSimFilter
	}
	field, op, raw := m[1], m[2], strings.TrimSpace(m[3])
	want := strings.Trim(raw, `"'`)

	var obj map[string]interface{}
	if attrs != "" {
		if err := json.Unmarshal([]byte(attrs), &obj); err != nil {
			return false, nil
		}
	}
	got, exists := obj[field]
	if !exists {
		return op == "!=", nil
	}
	equal := false
	switch v := got.(type) {
	case string:
		equal = v == want
	case float64:
		wf, err := strconv.ParseFloat(want, 64)
		if err == nil {
			equal = v == wf
		} else {
			equal = strconv.FormatFloat(v, 'f', -1, 64) == want
		}
	case bool:
		equal = strings.EqualFold(strconv.FormatBool(v), want) ||
			(want == "1" && v) || (want == "0" && !v)
	default:
		b, _ := json.Marshal(v)
		equal = strings.Trim(string(b), `"`) == want
	}
	if op == "==" {
		return equal, nil
	}
	return !equal, nil
}

var errInvalidVSimFilter = errors.New("invalid FILTER expression")

func formatVSimResults(results []*vector.SearchResult, withScores, withAttribs bool) redis.Reply {
	if len(results) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	out := make([][]byte, 0, len(results)*3)
	for _, r := range results {
		out = append(out, []byte(r.ID))
		if withScores {
			out = append(out, []byte(strconv.FormatFloat(float64(r.Score), 'f', -1, 32)))
		}
		if withAttribs {
			if r.Attributes != "" {
				out = append(out, []byte(r.Attributes))
			} else {
				out = append(out, nil) // null bulk in MultiBulkReply
			}
		}
	}
	return protocol.MakeMultiBulkReply(out)
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

// execVRandMember returns random element id(s) from a vector set.
// VRANDMEMBER key [count]
func execVRandMember(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 && len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vrandmember' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		if len(args) == 2 {
			return &protocol.EmptyMultiBulkReply{}
		}
		return &protocol.NullBulkReply{}
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	ids := make([]string, 0, vs.Len())
	vs.ForEach(func(id string, _ *vector.VectorItem) bool {
		ids = append(ids, id)
		return true
	})
	if len(ids) == 0 {
		if len(args) == 2 {
			return &protocol.EmptyMultiBulkReply{}
		}
		return &protocol.NullBulkReply{}
	}
	if len(args) == 1 {
		return protocol.MakeBulkReply([]byte(ids[rand.Intn(len(ids))]))
	}
	count64, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := int(count64)
	if count == 0 {
		return &protocol.EmptyMultiBulkReply{}
	}
	allowDup := count < 0
	if allowDup {
		count = -count
	}
	out := make([][]byte, count)
	if allowDup {
		for i := 0; i < count; i++ {
			out[i] = []byte(ids[rand.Intn(len(ids))])
		}
	} else {
		if count > len(ids) {
			count = len(ids)
		}
		perm := rand.Perm(len(ids))
		out = make([][]byte, count)
		for i := 0; i < count; i++ {
			out[i] = []byte(ids[perm[i]])
		}
	}
	return protocol.MakeMultiBulkReply(out)
}

// execVSetAttr sets JSON attributes on a vector element.
// VSETATTR key element json
func execVSetAttr(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vsetattr' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return protocol.MakeIntReply(0)
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	jsonAttr := string(args[2])
	if jsonAttr != "" {
		// Light validation: must look like JSON object/array or empty already handled.
		if jsonAttr[0] != '{' && jsonAttr[0] != '[' {
			return protocol.MakeErrReply("ERR invalid attribute format: must be a valid JSON object")
		}
	}
	if !vs.SetAttributes(string(args[1]), jsonAttr) {
		return protocol.MakeIntReply(0)
	}
	db.addAof(utils.ToCmdLine3("vsetattr", args...))
	return protocol.MakeIntReply(1)
}

// execVGetAttr returns JSON attributes.
// VGETATTR key element
func execVGetAttr(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vgetattr' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return &protocol.NullBulkReply{}
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	attr, ok := vs.GetAttributes(string(args[1]))
	if !ok || attr == "" {
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeBulkReply([]byte(attr))
}

// execVLinks returns HNSW neighbors (flat index: empty neighbor list).
// VLINKS key element [WITHSCORES]
func execVLinks(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 || len(args) > 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vlinks' command")
	}
	withScores := false
	if len(args) == 3 {
		if strings.ToUpper(string(args[2])) != "WITHSCORES" {
			return protocol.MakeSyntaxErrReply()
		}
		withScores = true
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return &protocol.NullBulkReply{}
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	if _, found := vs.Get(string(args[1])); !found {
		return &protocol.NullBulkReply{}
	}
	_ = withScores
	// No HNSW graph yet: one empty layer (compatible shape for clients).
	return protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeMultiBulkReply([][]byte{}),
	})
}

// execVRange returns elements in a lexicographical id range.
// VRANGE key start end [count]
func execVRange(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || len(args) > 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'vrange' command")
	}
	entity, exists := db.GetEntity(string(args[0]))
	if !exists {
		return &protocol.EmptyMultiBulkReply{}
	}
	vs, ok := entity.Data.(*vector.VectorSet)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	startSpec := string(args[1])
	endSpec := string(args[2])
	limit := -1
	if len(args) == 4 {
		n, err := strconv.Atoi(string(args[3]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		limit = n
	}
	ids := vs.SortedIDs()
	out := make([][]byte, 0)
	for _, id := range ids {
		if !lexRangeGE(id, startSpec) {
			continue
		}
		if !lexRangeLE(id, endSpec) {
			continue
		}
		out = append(out, []byte(id))
		if limit >= 0 && len(out) >= limit {
			break
		}
	}
	return protocol.MakeMultiBulkReply(out)
}

func lexRangeGE(id, spec string) bool {
	if spec == "-" {
		return true
	}
	if len(spec) == 0 {
		return true
	}
	switch spec[0] {
	case '[':
		return id >= spec[1:]
	case '(':
		return id > spec[1:]
	default:
		return id >= spec
	}
}

func lexRangeLE(id, spec string) bool {
	if spec == "+" {
		return true
	}
	if len(spec) == 0 {
		return true
	}
	switch spec[0] {
	case '[':
		return id <= spec[1:]
	case '(':
		return id < spec[1:]
	default:
		return id <= spec
	}
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
	registerCommand("VRandMember", execVRandMember, prepareVSKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VSetAttr", execVSetAttr, prepareVSKey, nil, 4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("VGetAttr", execVGetAttr, prepareVSKey, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VLinks", execVLinks, prepareVSKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("VRange", execVRange, prepareVSKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
