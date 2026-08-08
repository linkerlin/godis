package database

import (
	"bytes"
	"strconv"

	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// streamReadBucket is one stream's result slice for XREAD / XREADGROUP.
type streamReadBucket struct {
	key     string
	entries []*stream.StreamEntry
}

// StreamReadReply is the dual-form reply for XREAD / XREADGROUP.
//
// RESP2: [[key, entries], ...] — nested arrays (Redis wire form)
// RESP3: map { key => [[id, field-map], ...] }
type StreamReadReply struct {
	buckets []streamReadBucket
}

// MakeStreamReadReply builds a StreamReadReply from ordered stream buckets.
func MakeStreamReadReply(buckets []streamReadBucket) *StreamReadReply {
	return &StreamReadReply{buckets: buckets}
}

// ToBytes marshals RESP2 nested arrays (true nesting, not bulk-encoded blobs).
func (r *StreamReadReply) ToBytes() []byte {
	if r == nil || len(r.buckets) == 0 {
		return protocol.MakeEmptyMultiBulkReply().ToBytes()
	}
	parts := make([]redis.Reply, 0, len(r.buckets))
	for _, b := range r.buckets {
		parts = append(parts, protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeBulkReply([]byte(b.key)),
			streamEntriesToReply(b.entries),
		}))
	}
	return protocol.MakeMultiRawReply(parts).ToBytes()
}

// ToRESP3 marshals a RESP3 map of stream → entry array (fields as maps).
func (r *StreamReadReply) ToRESP3() []byte {
	if r == nil || len(r.buckets) == 0 {
		return []byte("%0\r\n")
	}
	var buf bytes.Buffer
	buf.WriteString("%" + strconv.Itoa(len(r.buckets)) + "\r\n")
	for _, b := range r.buckets {
		buf.Write(protocol.MakeBulkReply([]byte(b.key)).ToBytes())
		buf.WriteString("*" + strconv.Itoa(len(b.entries)) + "\r\n")
		for _, entry := range b.entries {
			buf.WriteString("*2\r\n")
			buf.Write(protocol.MakeBulkReply([]byte(entry.ID.String())).ToBytes())
			buf.Write(streamFieldsToRESP3Map(entry.Fields))
		}
	}
	return buf.Bytes()
}

func streamFieldsToRESP3Map(fields map[string]string) []byte {
	var buf bytes.Buffer
	buf.WriteString("%" + strconv.Itoa(len(fields)) + "\r\n")
	for k, v := range fields {
		buf.Write(protocol.MakeBulkReply([]byte(k)).ToBytes())
		buf.Write(protocol.MakeBulkReply([]byte(v)).ToBytes())
	}
	return buf.Bytes()
}

var (
	_ redis.Reply         = (*StreamReadReply)(nil)
	_ protocol.RESP3Reply = (*StreamReadReply)(nil)
)
