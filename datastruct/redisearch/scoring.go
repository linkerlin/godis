package redisearch

import (
	"fmt"
	"math"
	"strings"
)

// ScoreExplanation is the EXPLAINSCORE payload for one hit. Wire encoding
// happens in database/ (simple-string leaves + nested arrays matching Redis
// 8.x shape). Numbers use Godis BM25 params (k1/b); self-consistent with our
// scorer, not byte-identical to RediSearch (which defaults b=0.75).
type ScoreExplanation struct {
	Leaf       string   // DOCSCORE / DISMAX / fallback: single simple string
	Header     string   // BM25: "Final BM25 : words BM25 …"
	WeightLine string   // BM25 multi-term: "(Weight 1.00 * children BM25 …)"
	TermLines  []string // BM25 per-term breakdown lines
}

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
	payload    []byte  // FT.SEARCH PAYLOAD value (HAMMING scorer)
	tanhFactor float64 // BM25STD.TANH divisor; 0 → default 4
}

// computeScore dispatches to the scorer named by opts.Scorer (default BM25STD).
// docScore is the document's presumptive score (FT.ADD SCORE / SCORE_FIELD),
// which BM25STD/TFIDF multiply into the final result.
func (e *RediSearchEngine) computeScore(doc *Document, sc *scoreContext, scorerName string) float64 {
	switch strings.ToUpper(scorerName) {
	case ScorerBM25, ScorerBM25STD:
		return scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
	case ScorerBM25STDTanh:
		// BM25STD normalized via tanh(x/factor); default factor 4
		// (overridable via FT.SEARCH BM25STD_TANH_FACTOR).
		raw := scorerBM25STD(doc, sc, bm25K1, bm25B)
		factor := sc.tanhFactor
		if factor <= 0 {
			factor = 4
		}
		return math.Tanh(raw/factor) * doc.Score
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

// explainScore builds an EXPLAINSCORE tree for the named scorer. Subset of
// Redis shapes: BM25STD(/BM25), DOCSCORE, DISMAX; other scorers get a leaf.
func explainScore(doc *Document, sc *scoreContext, scorerName string) *ScoreExplanation {
	finalScore := float64(0)
	if sc != nil {
		// Match computeScore dispatch without needing the engine receiver.
		switch strings.ToUpper(scorerName) {
		case ScorerBM25, ScorerBM25STD, "":
			finalScore = scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
		case ScorerBM25STDTanh:
			raw := scorerBM25STD(doc, sc, bm25K1, bm25B)
			factor := sc.tanhFactor
			if factor <= 0 {
				factor = 4
			}
			finalScore = math.Tanh(raw/factor) * doc.Score
		case ScorerBM25STDNorm:
			finalScore = scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
		case ScorerDISMAX:
			finalScore = scorerDISMAX(doc, sc) * doc.Score
		case ScorerDOCSCORE:
			finalScore = doc.Score
		default:
			finalScore = scorerBM25STD(doc, sc, bm25K1, bm25B) * doc.Score
		}
	}
	switch strings.ToUpper(scorerName) {
	case "", ScorerBM25, ScorerBM25STD, ScorerBM25STDNorm, ScorerBM25STDTanh:
		return explainBM25STD(doc, sc)
	case ScorerTFIDF, ScorerTFIDFDocNorm:
		return explainTFIDF(doc, sc, strings.EqualFold(scorerName, ScorerTFIDFDocNorm))
	case ScorerDOCSCORE:
		return &ScoreExplanation{Leaf: fmt.Sprintf("Document's score is %.2f", doc.Score)}
	case ScorerDISMAX:
		freq := scorerDISMAX(doc, sc)
		return &ScoreExplanation{Leaf: fmt.Sprintf("DISMAX %.2f = Weight 1.00 * Frequency %.0f", finalScore, freq)}
	default:
		return &ScoreExplanation{Leaf: fmt.Sprintf("Score %.2f", finalScore)}
	}
}

func explainTFIDF(doc *Document, sc *scoreContext, docNorm bool) *ScoreExplanation {
	words := scorerTFIDF(doc, sc, docNorm)
	norm := 1
	if docNorm && sc != nil && sc.idx != nil {
		dl := sc.idx.docLengths[doc.ID]
		if dl > 0 {
			norm = dl
		}
	}
	docScore := 1.0
	if doc != nil {
		docScore = doc.Score
	}
	header := fmt.Sprintf(
		"Final TFIDF : words TFIDF %.2f * document score %.2f / norm %d / slop 1",
		words, docScore, norm,
	)
	var termLines []string
	if sc != nil && sc.docCount > 0 {
		for _, term := range sc.queryTerms {
			tf := float64(len(sc.idx.terms[term][doc.ID]))
			if tf == 0 {
				continue
			}
			df := float64(len(sc.idx.terms[term]))
			idf := math.Log((sc.docCount-df+0.5)/(df+0.5) + 1)
			contrib := tf * idf
			if docNorm {
				dl := float64(sc.idx.docLengths[doc.ID])
				if dl > 0 {
					contrib /= dl
				}
			}
			termLines = append(termLines, fmt.Sprintf(
				"(TFIDF %.2f = Weight 1.00 * TF %.0f * IDF %.2f)", contrib, tf, idf,
			))
		}
	}
	return &ScoreExplanation{Header: header, TermLines: termLines}
}

func explainBM25STD(doc *Document, sc *scoreContext) *ScoreExplanation {
	if sc == nil || sc.docCount == 0 {
		return &ScoreExplanation{Leaf: "Final BM25 : words BM25 0.00 * document score 1.00"}
	}
	words := scorerBM25STD(doc, sc, bm25K1, bm25B)
	header := fmt.Sprintf("Final BM25 : words BM25 %.2f * document score %.2f", words, doc.Score)
	dl := float64(sc.idx.docLengths[doc.ID])
	avgdl := sc.avgdl
	if avgdl <= 0 {
		avgdl = 1
	}
	denomBase := bm25K1 * (1 - bm25B + bm25B*dl/avgdl)
	var termLines []string
	for _, term := range sc.queryTerms {
		idfDenom := float64(len(sc.idx.terms[term]))
		if idfDenom == 0 {
			continue
		}
		idf := math.Log((sc.docCount-idfDenom+0.5)/(idfDenom+0.5) + 1)
		var contrib, tf, weight float64
		weight = 1
		usedField := false
		if !sc.idx.noFields {
			for _, field := range sc.idx.fields {
				if field == nil || field.Type != FieldTypeText || field.NoIndex {
					continue
				}
				ftf := float64(len(sc.idx.terms[field.Name+":"+term][doc.ID]))
				if ftf == 0 {
					continue
				}
				w := field.Weight
				if w <= 0 {
					w = 1
				}
				c := w * idf * (ftf * (bm25K1 + 1)) / (ftf + denomBase)
				contrib += c
				if !usedField || ftf*w > tf*weight {
					tf, weight = ftf, w
				}
				usedField = true
			}
		}
		if !usedField {
			tf = float64(len(sc.idx.terms[term][doc.ID]))
			if tf == 0 {
				continue
			}
			contrib = idf * (tf * (bm25K1 + 1)) / (tf + denomBase)
			weight = 1
		}
		termLines = append(termLines, fmt.Sprintf(
			"%s: (%.2f = Weight %.2f * IDF %.2f * (F %.2f * (k1 %.1f + 1)) / (F %.2f + k1 %.1f * (1 - b %g + b %g * Doc Len %.0f / Average Doc Len %.2f)))",
			term, contrib, weight, idf, tf, bm25K1, tf, bm25K1, bm25B, bm25B, dl, avgdl,
		))
	}
	ex := &ScoreExplanation{Header: header, TermLines: termLines}
	if len(termLines) > 1 {
		ex.WeightLine = fmt.Sprintf("(Weight 1.00 * children BM25 %.2f)", words)
	}
	return ex
}
