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
	// XREADGROUP CLAIM (8.4) extras for the claimed prefix
	// entries[0:claimedCount]; the reply then carries
	// [id, fields, idle-ms, delivery-count] per claimed entry.
	claimedCount int
	claimIdle    []int64 // ms since last delivery at claim time; -1 = XNACK-released
	claimDeliver []int
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
			bucketEntriesToReply(&b),
		}))
	}
	return protocol.MakeMultiRawReply(parts).ToBytes()
}

// bucketEntriesToReply marshals one bucket's entries; the claimed prefix
// (XREADGROUP CLAIM) uses the extended [id, fields, idle-ms, deliveries]
// form, regular entries the plain [id, fields] form.
func bucketEntriesToReply(b *streamReadBucket) redis.Reply {
	if len(b.entries) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	subReplies := make([]redis.Reply, 0, len(b.entries))
	for i, entry := range b.entries {
		idFields := []redis.Reply{
			protocol.MakeBulkReply([]byte(entry.ID.String())),
			streamFieldsToReply(entry.Fields),
		}
		if i < b.claimedCount {
			idFields = append(idFields,
				protocol.MakeIntReply(b.claimIdle[i]),
				protocol.MakeIntReply(int64(b.claimDeliver[i])))
		}
		subReplies = append(subReplies, protocol.MakeMultiRawReply(idFields))
	}
	return protocol.MakeMultiRawReply(subReplies)
}

// streamFieldsToReply flattens field/value pairs (RESP2 array form).
func streamFieldsToReply(fields map[string]string) redis.Reply {
	if len(fields) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	pairs := make([]redis.Reply, 0, len(fields)*2)
	for k, v := range fields {
		pairs = append(pairs,
			protocol.MakeBulkReply([]byte(k)),
			protocol.MakeBulkReply([]byte(v)))
	}
	return protocol.MakeMultiRawReply(pairs)
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
		for i, entry := range b.entries {
			if i < b.claimedCount {
				buf.WriteString("*4\r\n")
			} else {
				buf.WriteString("*2\r\n")
			}
			buf.Write(protocol.MakeBulkReply([]byte(entry.ID.String())).ToBytes())
			buf.Write(streamFieldsToRESP3Map(entry.Fields))
			if i < b.claimedCount {
				buf.Write(protocol.MakeIntReply(b.claimIdle[i]).ToBytes())
				buf.Write(protocol.MakeIntReply(int64(b.claimDeliver[i])).ToBytes())
			}
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
