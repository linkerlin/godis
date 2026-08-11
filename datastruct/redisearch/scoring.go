package redisearch

import (
	"math"
	"strings"
)

// Scorer names recognized by FT.SEARCH SCORER. BM25STD is the Redis 8.x
// default (renamed from BM25 in 8.4; BM25 is a deprecated alias).
const (
	ScorerBM25STD      = "BM25STD"
	ScorerBM25         = "BM25"        // deprecated alias of BM25STD
	ScorerBM25STDNorm  = "BM25STD.NORM"
	ScorerBM25STDTanh  = "BM25STD.TANH"
	ScorerTFIDF        = "TFIDF"
	ScorerTFIDFDocNorm = "TFIDF.DOCNORM"
	ScorerDISMAX       = "DISMAX"
	ScorerDOCSCORE     = "DOCSCORE"
	ScorerHAMMING      = "HAMMING"
)

// BM25 parameters used by BM25STD and its variants. k1 controls term-frequency
// saturation; b controls length normalization. These match RediSearch defaults.
const (
	bm25K1 = 1.2
	bm25B  = 0.09
)

// scoreContext bundles the per-query statistics a scorer needs. Computed once
// per Search call so each doc scores against the same corpus snapshot.
type scoreContext struct {
	idx        *InvertedIndex
	queryTerms []string // stemmed/filtered query tokens (may include optional)
	optional   map[string]bool
	docCount   float64
	avgdl      float64
	payload    []byte // FT.SEARCH PAYLOAD value (HAMMING scorer)
}

// computeScore dispatches to the scorer named by opts.Scorer (default BM25STD).
// docScore is the document's presumptive score (FT.ADD SCORE / SCORE_FIELD),
// which BM25STD/TFIDF multiply into the final result.
func (e *RediSearchEngine) computeScore(doc *Document, sc *scoreContext, scorerName string) float64 {
	switch strings.ToUpper(scorerName) {
	case ScorerBM25, ScorerBM25STD:
		return scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
	case ScorerBM25STDTanh:
		// BM25STD normalized via tanh(x/factor); default factor 4.
		raw := scorerBM25STD(doc, sc, bm25K1, bm25B)
		return math.Tanh(raw/4.0) * doc.Score
	case ScorerBM25STDNorm:
		// Raw BM25; Search applies true min-max over the full hit set
		// (see normalizeBM25STDNorm).
		return scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
	case ScorerTFIDF:
		return scorerTFIDF(doc, sc, false) * doc.Score
	case ScorerTFIDFDocNorm:
		return scorerTFIDF(doc, sc, true) * doc.Score
	case ScorerDISMAX:
		return scorerDISMAX(doc, sc) * doc.Score
	case ScorerDOCSCORE:
		return doc.Score
	case ScorerHAMMING:
		return scorerHAMMING(doc, sc)
	default:
		// Unknown scorer falls back to the default (BM25STD) rather than zero,
		// so a typo in SCORER doesn't silently derank everything.
		return scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
	}
}

// normalizeBM25STDNorm rescales BM25STD.NORM scores to [0,1] via min-max over
// the current result set. Equal scores map to 1 (all equally relevant).
func normalizeBM25STDNorm(results []*SearchResult) {
	if len(results) == 0 {
		return
	}
	min, max := results[0].Score, results[0].Score
	for _, r := range results[1:] {
		if r.Score < min {
			min = r.Score
		}
		if r.Score > max {
			max = r.Score
		}
	}
	if max == min {
		for _, r := range results {
			r.Score = 1
		}
		return
	}
	span := max - min
	for _, r := range results {
		r.Score = (r.Score - min) / span
	}
}

// scorerBM25STD computes BM25 with the given saturation/normalization params.
// score = Σ_t Σ_f [ w(f) · idf(t) · tf_f(t)·(k1+1) / (tf_f(t) + k1·(1 - b + b·dl/avgdl)) ]
// where idf(t) = ln((N - df + 0.5)/(df + 0.5) + 1) uses the global term df.
// TEXT field WEIGHT multiplies each field's contribution (default 1.0). When
// NOFIELDS suppressed field postings, falls back to the unscoped term list.
func scorerBM25STD(doc *Document, sc *scoreContext, k1, b float64) float64 {
	if sc.docCount == 0 || len(sc.queryTerms) == 0 {
		return 0
	}
	dl := float64(sc.idx.docLengths[doc.ID])
	var score float64
	avgdl := sc.avgdl
	if avgdl <= 0 {
		// Empty corpus length (NOFREQS/empty TEXT): avoid Inf; length term → 1-b.
		avgdl = 1
	}
	for _, term := range sc.queryTerms {
		idfDenom := float64(len(sc.idx.terms[term]))
		if idfDenom == 0 {
			continue
		}
		idf := math.Log((sc.docCount-idfDenom+0.5)/(idfDenom+0.5) + 1)
		denomBase := k1 * (1 - b + b*dl/avgdl)
		fieldContrib := 0.0
		usedField := false
		if !sc.idx.noFields {
			for _, field := range sc.idx.fields {
				if field == nil || field.Type != FieldTypeText || field.NoIndex {
					continue
				}
				tf := float64(len(sc.idx.terms[field.Name+":"+term][doc.ID]))
				if tf == 0 {
					continue
				}
				w := field.Weight
				if w <= 0 {
					w = 1
				}
				fieldContrib += w * idf * (tf * (k1 + 1)) / (tf + denomBase)
				usedField = true
			}
		}
		if usedField {
			score += fieldContrib
			continue
		}
		tf := float64(len(sc.idx.terms[term][doc.ID]))
		if tf == 0 {
			continue
		}
		score += idf * (tf * (k1 + 1)) / (tf + denomBase)
	}
	return score
}

// scorerTFIDF computes classic TF-IDF. When docNorm is true, the per-term
// contribution is divided by the document's weighted length (TFIDF.DOCNORM).
func scorerTFIDF(doc *Document, sc *scoreContext, docNorm bool) float64 {
	if sc.docCount == 0 {
		return 0
	}
	var score float64
	for _, term := range sc.queryTerms {
		tf := float64(len(sc.idx.terms[term][doc.ID]))
		if tf == 0 {
			continue
		}
		df := float64(len(sc.idx.terms[term]))
		idf := math.Log((sc.docCount-df+0.5)/(df+0.5) + 1)
		score += tf * idf
	}
	if docNorm {
		dl := float64(sc.idx.docLengths[doc.ID])
		if dl > 0 {
			score /= dl
		}
	}
	return score
}

// scorerDISMAX sums matched term frequencies (union = max across query terms,
// approximated here as sum since we don't track union semantics per field).
func scorerDISMAX(doc *Document, sc *scoreContext) float64 {
	var score float64
	for _, term := range sc.queryTerms {
		tf := float64(len(sc.idx.terms[term][doc.ID]))
		score += tf
	}
	return score
}

// scorerHAMMING returns 1/(1+Hamming distance) between the doc payload and the
// query payload (sc.payload). Both must be present and of equal length per
// Redis semantics; otherwise the doc scores 0.
func scorerHAMMING(doc *Document, sc *scoreContext) float64 {
	if len(sc.payload) == 0 || len(doc.Payload) == 0 || len(sc.payload) != len(doc.Payload) {
		return 0
	}
	dist := 0
	for i := range sc.payload {
		if sc.payload[i] != doc.Payload[i] {
			dist++
		}
	}
	return 1.0 / (1.0 + float64(dist))
}
