package database

import (
	"strconv"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// FTSearchReply is the dual-form reply for FT.SEARCH. RESP2 connections see the
// classic positional array ([total, id, fields, id, fields, ...], unchanged for
// backward compat); RESP3 connections see the Redis 8.x map shape:
//
//	%5
//	  $13 total_results :N
//	  $7  results       *M [ ...per-doc maps... ]
//	  $10 attributes    *K [ ...returned field names... ]
//	  $6  format        $4 text
//	  $7  warning       *0
//
// It wraps the already-built RESP2 reply plus the option flags needed to walk
// the positional array and rebuild it as a map.
type FTSearchReply struct {
	resp2            redis.Reply // the positional RESP2 array (MultiRawReply or cursor form)
	total            int64
	withScores       bool
	withPayloads     bool
	withSortKeys     bool
	withExplainScore bool // score slot is [score, explain] nested array
	noContent        bool
	attributes       []string // RETURN field names (nil = all fields)
}

// MakeFTSearchReply wraps a RESP2 FT.SEARCH reply with the flags ToRESP3 needs.
func MakeFTSearchReply(resp2 redis.Reply, total int64, withScores, withPayloads, withSortKeys, noContent bool, attributes []string) *FTSearchReply {
	return &FTSearchReply{
		resp2:        resp2,
		total:        total,
		withScores:   withScores,
		withPayloads: withPayloads,
		withSortKeys: withSortKeys,
		noContent:    noContent,
		attributes:   attributes,
	}
}

// setExplainScore marks that WITHSCORES+EXPLAINSCORE nested the score slot.
func (r *FTSearchReply) setExplainScore(v bool) *FTSearchReply {
	if r != nil {
		r.withExplainScore = v
	}
	return r
}

// ToBytes returns the RESP2 positional array — identical to pre-RESP3 behavior.
func (r *FTSearchReply) ToBytes() []byte {
	if r == nil || r.resp2 == nil {
		return []byte("*0\r\n")
	}
	return r.resp2.ToBytes()
}

// ToRESP3 returns the Redis 8.x map form for RESP3 connections.
func (r *FTSearchReply) ToRESP3() []byte {
	total := r.total
	docs := r.extractDocs()

	// results: array of per-doc maps.
	resultsBytes := buildRESP3ArrayLen(len(docs))
	for _, d := range docs {
		resultsBytes = append(resultsBytes, d.toRESP3Map()...)
	}

	// attributes: the RETURN field names (or empty when not projecting).
	attrsBytes := buildRESP3ArrayLen(len(r.attributes))
	for _, a := range r.attributes {
		attrsBytes = append(attrsBytes, protocol.MakeBulkReply([]byte(a)).ToBytes()...)
	}

	// Top-level map: total_results, results, attributes, format, warning.
	out := []byte("%5\r\n")
	out = append(out, protocol.MakeBulkReply([]byte("total_results")).ToBytes()...)
	out = append(out, []byte(":"+strconv.FormatInt(total, 10)+"\r\n")...)
	out = append(out, protocol.MakeBulkReply([]byte("results")).ToBytes()...)
	out = append(out, resultsBytes...)
	out = append(out, protocol.MakeBulkReply([]byte("attributes")).ToBytes()...)
	out = append(out, attrsBytes...)
	out = append(out, protocol.MakeBulkReply([]byte("format")).ToBytes()...)
	out = append(out, protocol.MakeBulkReply([]byte("text")).ToBytes()...)
	out = append(out, protocol.MakeBulkReply([]byte("warning")).ToBytes()...)
	out = append(out, []byte("*0\r\n")...)
	return out
}

// ftDoc is one flattened search result extracted from the RESP2 positional
// array, carrying only the fields actually present per the request flags.
type ftDoc struct {
	id           string
	score        string // valid when withScores && !explain
	scoreRESP    []byte // when EXPLAINSCORE: full [score, explain] array bytes
	payload      []byte // valid when withPayloads
	sortKey      string // valid when withSortKeys
	fields       []byte // the nested fields array bytes (valid when !noContent)
	hasScore     bool
	hasScoreNest bool
	hasPayload   bool
	hasSortKey   bool
	hasFields    bool
}

func (d *ftDoc) toRESP3Map() []byte {
	// Count map entries: id always; then each present extra.
	n := 1
	if d.hasScore || d.hasScoreNest {
		n++
	}
	if d.hasPayload {
		n++
	}
	if d.hasSortKey {
		n++
	}
	if d.hasFields {
		n++
	}
	out := []byte("%" + strconv.Itoa(n) + "\r\n")
	out = append(out, protocol.MakeBulkReply([]byte("id")).ToBytes()...)
	out = append(out, protocol.MakeBulkReply([]byte(d.id)).ToBytes()...)
	if d.hasScoreNest {
		out = append(out, protocol.MakeBulkReply([]byte("score")).ToBytes()...)
		out = append(out, d.scoreRESP...)
	} else if d.hasScore {
		out = append(out, protocol.MakeBulkReply([]byte("score")).ToBytes()...)
		out = append(out, protocol.MakeBulkReply([]byte(d.score)).ToBytes()...)
	}
	if d.hasPayload {
		out = append(out, protocol.MakeBulkReply([]byte("payload")).ToBytes()...)
		out = append(out, protocol.MakeBulkReply(d.payload).ToBytes()...)
	}
	if d.hasSortKey {
		out = append(out, protocol.MakeBulkReply([]byte("sortkey")).ToBytes()...)
		out = append(out, protocol.MakeBulkReply([]byte(d.sortKey)).ToBytes()...)
	}
	if d.hasFields {
		out = append(out, protocol.MakeBulkReply([]byte("extra_attributes")).ToBytes()...)
		// The fields blob is already a RESP2/RESP3 array (MakeMultiBulkReply
		// bytes); emit it verbatim — arrays serialize the same in both protocols.
		out = append(out, d.fields...)
	}
	return out
}

// extractDocs walks the wrapped RESP2 positional array and returns one ftDoc
// per result, using the request flags to determine the per-doc stride.
func (r *FTSearchReply) extractDocs() []ftDoc {
	if r.resp2 == nil {
		return nil
	}
	mr, ok := r.resp2.(*protocol.MultiRawReply)
	if !ok {
		return nil
	}
	if len(mr.Replies) < 1 {
		return nil
	}
	// Replies[0] = total; docs start at index 1. Stride per doc:
	// id(1) + score? + payload? + sortkey? + fields?
	stride := 1
	if r.withScores {
		stride++
	}
	if r.withPayloads {
		stride++
	}
	if r.withSortKeys {
		stride++
	}
	if !r.noContent {
		stride++
	}
	var docs []ftDoc
	for i := 1; i+stride <= len(mr.Replies)+1 && i < len(mr.Replies); i += stride {
		d := ftDoc{}
		idx := i
		if id, ok := mr.Replies[idx].(*protocol.BulkReply); ok {
			d.id = string(id.Arg)
		}
		idx++
		if r.withScores && idx < len(mr.Replies) {
			switch slot := mr.Replies[idx].(type) {
			case *protocol.BulkReply:
				d.score = string(slot.Arg)
				d.hasScore = true
			case *protocol.MultiRawReply:
				// EXPLAINSCORE: [scoreBulk, explain...] — emit nested array in RESP3.
				d.scoreRESP = slot.ToBytes()
				d.hasScoreNest = true
				if len(slot.Replies) > 0 {
					if s, ok := slot.Replies[0].(*protocol.BulkReply); ok {
						d.score = string(s.Arg)
					}
				}
			}
			idx++
		}
		if r.withPayloads && idx < len(mr.Replies) {
			if p, ok := mr.Replies[idx].(*protocol.BulkReply); ok {
				d.payload = p.Arg
				d.hasPayload = true
			}
			idx++
		}
		if r.withSortKeys && idx < len(mr.Replies) {
			if sk, ok := mr.Replies[idx].(*protocol.BulkReply); ok {
				d.sortKey = string(sk.Arg)
				d.hasSortKey = true
			}
			idx++
		}
		if !r.noContent && idx < len(mr.Replies) {
			d.fields = mr.Replies[idx].ToBytes()
			d.hasFields = true
		}
		docs = append(docs, d)
	}
	return docs
}

// buildRESP3ArrayLen emits a RESP3/RESP2 array header "*N\r\n" (identical in
// both protocols).
func buildRESP3ArrayLen(n int) []byte {
	return []byte("*" + strconv.Itoa(n) + "\r\n")
}

// FTAggregateReply is the dual-form reply for FT.AGGREGATE. RESP2 connections
// see the classic flat array ([total, row1, row2, ...]); RESP3 connections see
// the Redis 8.x map shape with total_results / results / attributes / format /
// warning. Rows are emitted verbatim (they are already RESP arrays of k/v
// pairs); a future refinement can re-parse each row into a per-row map.
type FTAggregateReply struct {
	resp2 redis.Reply // the flat RESP2 array (MultiBulkReply)
	total int64
}

// MakeFTAggregateReply wraps a RESP2 FT.AGGREGATE reply for dual-form output.
func MakeFTAggregateReply(resp2 redis.Reply, total int64) *FTAggregateReply {
	return &FTAggregateReply{resp2: resp2, total: total}
}

// ToBytes returns the RESP2 flat array — unchanged.
func (r *FTAggregateReply) ToBytes() []byte {
	if r == nil || r.resp2 == nil {
		return []byte("*0\r\n")
	}
	return r.resp2.ToBytes()
}

// ToRESP3 returns the Redis 8.x map form for RESP3 connections.
func (r *FTAggregateReply) ToRESP3() []byte {
	rowCount := 0
	var rowsBytes []byte
	if mb, ok := r.resp2.(*protocol.MultiBulkReply); ok {
		// Args[0] = total; Args[1..] = serialized row arrays.
		if len(mb.Args) > 1 {
			rowCount = len(mb.Args) - 1
			for _, row := range mb.Args[1:] {
				rowsBytes = append(rowsBytes, row...)
			}
		}
	}
	out := []byte("%5\r\n")
	out = append(out, protocol.MakeBulkReply([]byte("total_results")).ToBytes()...)
	out = append(out, []byte(":"+strconv.FormatInt(r.total, 10)+"\r\n")...)
	out = append(out, protocol.MakeBulkReply([]byte("results")).ToBytes()...)
	out = append(out, buildRESP3ArrayLen(rowCount)...)
	out = append(out, rowsBytes...)
	out = append(out, protocol.MakeBulkReply([]byte("attributes")).ToBytes()...)
	out = append(out, []byte("*0\r\n")...)
	out = append(out, protocol.MakeBulkReply([]byte("format")).ToBytes()...)
	out = append(out, protocol.MakeBulkReply([]byte("text")).ToBytes()...)
	out = append(out, protocol.MakeBulkReply([]byte("warning")).ToBytes()...)
	out = append(out, []byte("*0\r\n")...)
	return out
}
