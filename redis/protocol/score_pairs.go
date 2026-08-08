package protocol

import (
	"bytes"
	"strconv"

	"github.com/linkerlin/godis/interface/redis"
)

// ScorePairsReply encodes a list of (member, score) pairs.
// RESP2 wire form is always a flat array: [member, score, member, score, ...].
// RESP3 wire form is either:
//   - Nest=true:  [[member, score], ...]  (ZRANGE WITHSCORES, ZPOP with COUNT)
//   - Nest=false: [member, score, ...] with Double scores (ZPOP without COUNT)
type ScorePairsReply struct {
	Members []string
	Scores  []float64
	Nest    bool
}

// MakeScorePairsReply builds a ScorePairsReply. len(members) must equal len(scores).
func MakeScorePairsReply(members []string, scores []float64, nest bool) *ScorePairsReply {
	return &ScorePairsReply{Members: members, Scores: scores, Nest: nest}
}

// ToBytes marshals as RESP2 flat array of bulk strings.
func (r *ScorePairsReply) ToBytes() []byte {
	n := len(r.Members)
	args := make([][]byte, 0, n*2)
	for i := 0; i < n; i++ {
		args = append(args, []byte(r.Members[i]))
		args = append(args, []byte(formatFloat(r.Scores[i])))
	}
	return MakeMultiBulkReply(args).ToBytes()
}

// ToRESP3 marshals with Double scores; nested when Nest is set.
func (r *ScorePairsReply) ToRESP3() []byte {
	n := len(r.Members)
	var buf bytes.Buffer
	if r.Nest {
		buf.WriteString("*" + strconv.Itoa(n) + CRLF)
		for i := 0; i < n; i++ {
			buf.WriteString("*2\r\n")
			buf.Write(MakeBulkReply([]byte(r.Members[i])).ToBytes())
			buf.Write(MakeDoubleReply(r.Scores[i]).ToRESP3())
		}
		return buf.Bytes()
	}
	buf.WriteString("*" + strconv.Itoa(n*2) + CRLF)
	for i := 0; i < n; i++ {
		buf.Write(MakeBulkReply([]byte(r.Members[i])).ToBytes())
		buf.Write(MakeDoubleReply(r.Scores[i]).ToRESP3())
	}
	return buf.Bytes()
}

var (
	_ redis.Reply = (*ScorePairsReply)(nil)
	_ RESP3Reply  = (*ScorePairsReply)(nil)
)
