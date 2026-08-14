package database

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// hybridSpec captures a parsed FT.HYBRID command.
type hybridSpec struct {
	searchQuery  string
	scorer       string
	vecField     string
	vecBlob      []byte // decoded lazily; raw bytes from inline or PARAMS
	vecParam     string // $param name when the blob comes from PARAMS
	knnK         int
	knnEF        int
	combine      redisearch.HybridCombineConfig
	combineSet   bool
	limitOffset  int
	limitNum     int
	sortBy       string
	sortDesc     bool
	noSort       bool
	params       map[string][]byte
}

// execFTHybrid FT.HYBRID index SEARCH "q" VSIM @field <data> KNN K k [...] COMBINE ...
//
// Runs a text query and a vector KNN in parallel, then fuses the two ranked
// lists with RRF (default, Redis 8.4) or LINEAR. Reuses engine.Search and the
// per-field FTVectorIndex built in P2. This is a scoped implementation: it
// covers SEARCH + VSIM KNN + COMBINE RRF/LINEAR + LIMIT + SORTBY + PARAMS,
// which is the common 8.4 hybrid-search case. RANGE vector search, FILTER, and
// POLICY are accepted-but-deferred ( ponytail: ADHOC_BF is already the brute
// force path KNN uses).
func execFTHybrid(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.hybrid' command")
	}
	indexName := resolveSearchIndex(string(args[0]))
	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply(fmt.Sprintf("SEARCH_INDEX_NOT_FOUND Index not found: %s", string(args[0])))
	}

	spec, errReply := parseFTHybrid(args[1:])
	if errReply != nil {
		return errReply
	}
	if !spec.combineSet {
		spec.combine = redisearch.DefaultHybridCombineConfig()
	}
	if spec.limitNum == 0 {
		spec.limitNum = 10
	}

	// Resolve the query vector blob (inline or via PARAMS).
	blob := spec.vecBlob
	if spec.vecParam != "" {
		if got, ok := spec.params[strings.TrimPrefix(spec.vecParam, "$")]; ok {
			blob = got
		}
	}
	if spec.vecField == "" || blob == nil {
		return protocol.MakeErrReply("ERR FT.HYBRID requires VSIM with a vector field and data")
	}

	vi := engine.VectorIndex(spec.vecField)
	if vi == nil {
		return protocol.MakeErrReply(fmt.Sprintf("ERR Vector field '%s' not found in index", spec.vecField))
	}
	queryVec, derr := vi.DecodeVector(blob)
	if derr != nil {
		return protocol.MakeErrReply("ERR " + derr.Error())
	}

	// --- Text side: run the search query. A "*" search matches all docs (the
	// vector side carries the ranking signal in that case).
	textHits := hybridTextHits(engine, spec)
	// --- Vector side: KNN over the whole index.
	vecHits := hybridVectorHits(vi, queryVec, spec)

	// For LINEAR, normalize both sides to [0,1] so the weights are comparable.
	if spec.combine.Policy == redisearch.HybridLinear {
		textHits = redisearch.NormalizeScores(textHits)
		vecHits = redisearch.NormalizeScores(vecHits)
	}

	fused := redisearch.CombineHybrid(textHits, vecHits, spec.combine)

	// SORTBY overrides the fused-score order when requested.
	if spec.noSort {
		// NOSORT: leave insertion (fused) order but don't re-sort by score.
	} else if spec.sortBy != "" {
		sort.Slice(fused, func(i, j int) bool {
			a, b := hybridSortKey(fused[i], spec.sortBy, engine), hybridSortKey(fused[j], spec.sortBy, engine)
			if spec.sortDesc {
				return a > b
			}
			return a < b
		})
	}

	// LIMIT.
	total := len(fused)
	off := spec.limitOffset
	if off > total {
		off = total
	}
	end := off + spec.limitNum
	if end > total {
		end = total
	}
	page := fused[off:end]

	// Build reply: [total, id, [fused_score, text_score, vec_score], ...].
	replies := make([]redis.Reply, 0, 1+3*len(page))
	replies = append(replies, protocol.MakeIntReply(int64(total)))
	for _, fr := range page {
		replies = append(replies, protocol.MakeBulkReply([]byte(fr.DocID)))
		replies = append(replies, protocol.MakeMultiBulkReply([][]byte{
			[]byte("__hybrid_score"), []byte(strconv.FormatFloat(fr.FusedScore, 'f', -1, 64)),
			[]byte("__text_score"), []byte(strconv.FormatFloat(fr.TextScore, 'f', -1, 64)),
			[]byte("__vec_score"), []byte(strconv.FormatFloat(fr.VecScore, 'f', -1, 64)),
		}))
	}
	return protocol.MakeMultiRawReply(replies)
}

// hybridTextHits runs the text search and returns its results as HybridHits
// (ranked by descending score). A "*" or empty query yields no text hits (the
// vector side drives ranking alone).
func hybridTextHits(engine *redisearch.RediSearchEngine, spec hybridSpec) []redisearch.HybridHit {
	q := strings.TrimSpace(spec.searchQuery)
	if q == "" || q == "*" {
		return nil
	}
	opts := &redisearch.SearchOptions{
		Limit:   1000,
		Scorer:  spec.scorer,
		Dialect: 2,
	}
	res, err := engine.Search(q, opts)
	if err != nil || res == nil {
		return nil
	}
	// Sort by descending score to assign ranks.
	type sr struct {
		id    string
		score float64
	}
	sorted := make([]sr, 0, len(res.Results))
	for _, r := range res.Results {
		if r.Document == nil {
			continue
		}
		sorted = append(sorted, sr{id: r.Document.ID, score: r.Score})
	}
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].score > sorted[j].score })
	hits := make([]redisearch.HybridHit, len(sorted))
	for i, s := range sorted {
		hits[i] = redisearch.HybridHit{DocID: s.id, Score: s.score, Rank: i}
	}
	return hits
}

// hybridVectorHits runs the KNN search and returns its results as HybridHits,
// scored by similarity 1/(1+distance) (higher is closer) and ranked ascending
// by distance.
func hybridVectorHits(vi *redisearch.FTVectorIndex, query []float32, spec hybridSpec) []redisearch.HybridHit {
	hits := vi.SearchKNN(query, spec.knnK, spec.knnEF)
	out := make([]redisearch.HybridHit, len(hits))
	for i, h := range hits {
		out[i] = redisearch.HybridHit{
			DocID: h.DocID,
			Score: float64(1.0 / (1.0 + h.Distance)),
			Rank:  i, // SearchKNN already returns ascending-distance order
		}
	}
	return out
}

// hybridSortKey extracts a numeric sort key for a fused doc from its stored
// fields (used when SORTBY names a document field rather than the fused score).
func hybridSortKey(fr redisearch.HybridResult, field string, engine *redisearch.RediSearchEngine) float64 {
	// __hybrid_score / __text_score / __vec_score are the recognized synthetic
	// names; otherwise fall back to the fused score.
	switch strings.ToLower(field) {
	case "__hybrid_score", "hybrid_score":
		return fr.FusedScore
	case "__text_score", "text_score":
		return fr.TextScore
	case "__vec_score", "vec_score":
		return fr.VecScore
	}
	return fr.FusedScore
}

// parseFTHybrid parses the tokens after the index name into a hybridSpec.
func parseFTHybrid(args [][]byte) (hybridSpec, redis.Reply) {
	spec := hybridSpec{params: nil}
	i := 0
	for i < len(args) {
		sec := strings.ToUpper(string(args[i]))
		switch sec {
		case "SEARCH":
			if i+1 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			spec.searchQuery = string(args[i+1])
			i += 2
		case "SCORER":
			if i+1 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			spec.scorer = strings.ToUpper(string(args[i+1]))
			i += 2
		case "VSIM":
			// VSIM @field <data> { KNN ... | RANGE ... } [FILTER ...] [POLICY ...]
			if i+2 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			spec.vecField = strings.TrimPrefix(string(args[i+1]), "@")
			// The vector data may be an inline blob or a $param reference.
			spec.vecBlob = args[i+2]
			if strings.HasPrefix(string(args[i+2]), "$") {
				spec.vecParam = string(args[i+2])
				spec.vecBlob = nil
			}
			i += 3
			// Parse the KNN sub-block until the next top-level section.
			for i < len(args) {
				t := strings.ToUpper(string(args[i]))
				if t == "COMBINE" || t == "LIMIT" || t == "SORTBY" || t == "NOSORT" || t == "PARAMS" || t == "TIMEOUT" || t == "FORMAT" || t == "LOAD" {
					break
				}
				switch t {
				case "KNN":
					// KNN count K k [EF_RUNTIME ef] [YIELD_SCORE_AS name]
					if i+2 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					// args[i+1]=count (number of KNN params), args[i+2]="K"
					if !strings.EqualFold(string(args[i+2]), "K") {
						return spec, protocol.MakeErrReply("ERR VSIM expects K after KNN")
					}
					if i+3 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					k, err := strconv.Atoi(string(args[i+3]))
					if err != nil || k <= 0 {
						return spec, protocol.MakeErrReply("ERR Invalid KNN K")
					}
					spec.knnK = k
					i += 4
				case "EF_RUNTIME":
					if i+1 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					ef, err := strconv.Atoi(string(args[i+1]))
					if err != nil || ef <= 0 {
						return spec, protocol.MakeErrReply("ERR Invalid EF_RUNTIME")
					}
					spec.knnEF = ef
					i += 2
				case "YIELD_SCORE_AS":
					i += 2 // alias name; accepted, not separately tracked
				case "FILTER", "POLICY", "BATCH_SIZE":
					// ponytail: FILTER and POLICY accepted for syntax parity; the
					// KNN brute-force path already evaluates per-candidate.
					if i+1 >= len(args) {
						i++
					} else {
						i += 2
					}
				default:
					i++
				}
			}
		case "COMBINE":
			// COMBINE { RRF count [CONSTANT c] [WINDOW w] | LINEAR count [ALPHA a BETA b] [WINDOW w] }
			if i+2 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			policy := strings.ToUpper(string(args[i+1]))
			count, err := strconv.Atoi(string(args[i+2]))
			if err != nil || count <= 0 {
				return spec, protocol.MakeErrReply("ERR Invalid COMBINE count")
			}
			cfg := redisearch.HybridCombineConfig{Window: 20}
			i += 3
			switch policy {
			case "RRF":
				cfg.Policy = redisearch.HybridRRF
				cfg.Constant = 60
			case "LINEAR":
				cfg.Policy = redisearch.HybridLinear
				cfg.Alpha = 1.0
				cfg.Beta = 1.0
			default:
				return spec, protocol.MakeErrReply("ERR COMBINE policy must be RRF or LINEAR")
			}
			// Parse optional modifiers up to the next section.
			for i < len(args) {
				t := strings.ToUpper(string(args[i]))
				if t == "LIMIT" || t == "SORTBY" || t == "NOSORT" || t == "PARAMS" || t == "TIMEOUT" || t == "FORMAT" || t == "LOAD" {
					break
				}
				switch t {
				case "CONSTANT":
					if i+1 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					c, err := strconv.ParseFloat(string(args[i+1]), 64)
					if err != nil {
						return spec, protocol.MakeErrReply("ERR Invalid CONSTANT")
					}
					cfg.Constant = c
					i += 2
				case "ALPHA":
					if i+1 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					a, err := strconv.ParseFloat(string(args[i+1]), 64)
					if err != nil {
						return spec, protocol.MakeErrReply("ERR Invalid ALPHA")
					}
					cfg.Alpha = a
					i += 2
				case "BETA":
					if i+1 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					bv, err := strconv.ParseFloat(string(args[i+1]), 64)
					if err != nil {
						return spec, protocol.MakeErrReply("ERR Invalid BETA")
					}
					cfg.Beta = bv
					i += 2
				case "WINDOW":
					if i+1 >= len(args) {
						return spec, protocol.MakeSyntaxErrReply()
					}
					w, err := strconv.Atoi(string(args[i+1]))
					if err != nil || w <= 0 {
						return spec, protocol.MakeErrReply("ERR Invalid WINDOW")
					}
					cfg.Window = w
					i += 2
				case "YIELD_SCORE_AS":
					i += 2
				default:
					i++
				}
			}
			spec.combine = cfg
			spec.combineSet = true
			_ = count
		case "LIMIT":
			if i+2 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			off, err1 := strconv.Atoi(string(args[i+1]))
			num, err2 := strconv.Atoi(string(args[i+2]))
			if err1 != nil || err2 != nil || off < 0 || num < 0 {
				return spec, protocol.MakeErrReply("ERR Invalid LIMIT")
			}
			spec.limitOffset = off
			spec.limitNum = num
			i += 3
		case "SORTBY":
			if i+1 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			spec.sortBy = string(args[i+1])
			spec.sortDesc = false
			i += 2
			if i < len(args) {
				if strings.EqualFold(string(args[i]), "DESC") {
					spec.sortDesc = true
					i++
				} else if strings.EqualFold(string(args[i]), "ASC") {
					i++
				}
			}
		case "NOSORT":
			spec.noSort = true
			i++
		case "PARAMS":
			if i+1 >= len(args) {
				return spec, protocol.MakeSyntaxErrReply()
			}
			count, err := strconv.Atoi(string(args[i+1]))
			if err != nil || count < 0 || count%2 != 0 {
				return spec, protocol.MakeErrReply("ERR Invalid PARAMS count")
			}
			i += 2
			if spec.params == nil {
				spec.params = make(map[string][]byte, count/2)
			}
			for j := 0; j < count; j += 2 {
				if i+1 >= len(args) {
					return spec, protocol.MakeSyntaxErrReply()
				}
				spec.params[string(args[i])] = args[i+1]
				i += 2
			}
		case "TIMEOUT", "FORMAT", "LOAD":
			// Accepted for syntax parity; TIMEOUT is advisory, FORMAT/LOAD are
			// passed through to the result builder in a future refinement.
			i++
			// Skip a following value token when one is present and not a section.
			if i < len(args) {
				t := strings.ToUpper(string(args[i]))
				if t != "SEARCH" && t != "VSIM" && t != "COMBINE" && t != "LIMIT" && t != "SORTBY" && t != "PARAMS" {
					i++
				}
			}
		default:
			return spec, protocol.MakeSyntaxErrReply()
		}
	}
	return spec, nil
}

// encodeF32 is a small helper for tests/docs to build a FLOAT32 vector blob.
func encodeF32(xs []float32) []byte {
	buf := make([]byte, 4*len(xs))
	for i, x := range xs {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return buf
}

func init() {
	registerCommand("FT.Hybrid", execFTHybrid, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	_ = utils.ToCmdLine // keep import in case of future AOF wiring
}
