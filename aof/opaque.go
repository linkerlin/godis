package aof

import (
	"encoding/json"
	"math"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/redis/protocol"
)

// Godis opaque RDB/AOF encoding (not Redis-interoperable).
// Format: magic || JSON{type, data}
var opaqueMagic = []byte("GODIS1\x00")

const (
	opaqueStream     = "stream"
	opaqueJSON       = "json"
	opaqueVector     = "vector"
	opaqueTimeSeries = "ts"
	opaqueExpireDict = "hexpire"
	opaqueBloom      = "bloom"
	opaqueCuckoo     = "cuckoo"
	opaqueCMS        = "cms"
	opaqueTopK       = "topk"
	opaqueTDigest    = "tdigest"
)

type opaqueEnvelope struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

type streamDump struct {
	Entries []streamEntryDump `json:"e"`
	Groups  []streamGroupDump `json:"g"`
}

type streamEntryDump struct {
	ID     string            `json:"id"`
	Fields map[string]string `json:"f"`
}

type streamGroupDump struct {
	Name        string `json:"n"`
	LastID      string `json:"lid"`
	EntriesRead int64  `json:"er"`
}

type vectorDump struct {
	Dim   int               `json:"dim"`
	Items []vectorItemDump  `json:"items"`
}

type vectorItemDump struct {
	ID       string            `json:"id"`
	Data     []float64         `json:"d"`
	Metadata map[string]string `json:"m,omitempty"`
}

type tsDump struct {
	Key       string            `json:"k"`
	Retention int64             `json:"r"` // nanoseconds
	Labels    map[string]string `json:"l"`
	Samples   []tsSampleDump    `json:"s"`
}

type tsSampleDump struct {
	Timestamp int64   `json:"t"`
	Value     float64 `json:"v"`
}

// expireDictDump preserves hash field values + absolute expire times (unix ms).
type expireDictDump struct {
	Fields map[string][]byte `json:"f"`
	Expire map[string]int64  `json:"e,omitempty"` // field -> expire-at unix ms
}

// EncodeOpaque serializes Godis-specific types into a magic-prefixed blob.
// Returns ok=false for types that should use standard Redis encoding.
func EncodeOpaque(entity *database.DataEntity) (payload []byte, ok bool) {
	if entity == nil {
		return nil, false
	}
	var typ string
	var raw []byte
	var err error
	switch v := entity.Data.(type) {
	case *stream.Stream:
		typ = opaqueStream
		raw, err = json.Marshal(dumpStream(v))
	case *godisjson.JSONValue:
		typ = opaqueJSON
		raw, err = v.ToBytes()
	case *vector.VectorSet:
		typ = opaqueVector
		raw, err = json.Marshal(dumpVector(v))
	case *timeseries.TimeSeries:
		typ = opaqueTimeSeries
		raw, err = json.Marshal(dumpTimeSeries(v))
	case *dict.ExpireDict:
		typ = opaqueExpireDict
		raw, err = json.Marshal(dumpExpireDict(v))
	case *probabilistic.BloomFilter:
		typ = opaqueBloom
		raw, err = json.Marshal(v.MarshalBinary()) // base64 JSON string; binary is not RawMessage-safe
	case *probabilistic.CuckooFilter:
		typ = opaqueCuckoo
		raw, err = json.Marshal(v.MarshalBinary())
	case *probabilistic.CountMinSketch:
		typ = opaqueCMS
		raw, err = v.EncodeJSON()
	case *probabilistic.TopK:
		typ = opaqueTopK
		raw, err = v.EncodeJSON()
	case *probabilistic.TDigest:
		typ = opaqueTDigest
		raw, err = v.EncodeJSON()
	default:
		return nil, false
	}
	if err != nil {
		return nil, false
	}
	env, err := json.Marshal(opaqueEnvelope{Type: typ, Data: raw})
	if err != nil {
		return nil, false
	}
	out := make([]byte, 0, len(opaqueMagic)+len(env))
	out = append(out, opaqueMagic...)
	out = append(out, env...)
	return out, true
}

// DecodeOpaque restores a Godis opaque blob. ok=false if not opaque.
func DecodeOpaque(payload []byte) (entity *database.DataEntity, ok bool) {
	if len(payload) < len(opaqueMagic) || string(payload[:len(opaqueMagic)]) != string(opaqueMagic) {
		return nil, false
	}
	var env opaqueEnvelope
	if err := json.Unmarshal(payload[len(opaqueMagic):], &env); err != nil {
		return nil, false
	}
	switch env.Type {
	case opaqueStream:
		s, err := loadStream(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: s}, true
	case opaqueJSON:
		jv, err := godisjson.NewJSONValueFromBytes(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: jv}, true
	case opaqueVector:
		vs, err := loadVector(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: vs}, true
	case opaqueTimeSeries:
		ts, err := loadTimeSeries(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: ts}, true
	case opaqueExpireDict:
		ed, err := loadExpireDict(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: ed}, true
	case opaqueBloom:
		var bin []byte
		if err := json.Unmarshal(env.Data, &bin); err != nil {
			return nil, false
		}
		bf, err := probabilistic.UnmarshalBloomFilter(bin)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: bf}, true
	case opaqueCuckoo:
		var bin []byte
		if err := json.Unmarshal(env.Data, &bin); err != nil {
			return nil, false
		}
		cf, err := probabilistic.UnmarshalCuckooFilter(bin)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: cf}, true
	case opaqueCMS:
		cms, err := probabilistic.DecodeCountMinSketch(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: cms}, true
	case opaqueTopK:
		tk, err := probabilistic.DecodeTopK(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: tk}, true
	case opaqueTDigest:
		td, err := probabilistic.DecodeTDigest(env.Data)
		if err != nil {
			return nil, false
		}
		return &database.DataEntity{Data: td}, true
	default:
		return nil, false
	}
}

// IsOpaquePayload reports whether data looks like a Godis opaque blob.
func IsOpaquePayload(payload []byte) bool {
	return len(payload) >= len(opaqueMagic) && string(payload[:len(opaqueMagic)]) == string(opaqueMagic)
}

func dumpStream(s *stream.Stream) streamDump {
	d := streamDump{}
	for _, e := range s.Range(stream.StreamID{}, stream.StreamID{Timestamp: math.MaxInt64, Sequence: math.MaxInt64}) {
		fields := make(map[string]string, len(e.Fields))
		for k, v := range e.Fields {
			fields[k] = v
		}
		d.Entries = append(d.Entries, streamEntryDump{ID: e.ID.String(), Fields: fields})
	}
	for _, g := range s.GetGroups() {
		d.Groups = append(d.Groups, streamGroupDump{
			Name:        g.Name,
			LastID:      g.LastID.String(),
			EntriesRead: g.EntriesRead,
		})
	}
	return d
}

func loadStream(raw []byte) (*stream.Stream, error) {
	var d streamDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	s := stream.NewStream()
	for _, e := range d.Entries {
		if _, err := s.Add(e.ID, e.Fields, nil); err != nil {
			return nil, err
		}
	}
	for _, g := range d.Groups {
		if err := s.CreateGroup(g.Name, g.LastID); err != nil {
			return nil, err
		}
		grp, err := s.GetGroup(g.Name)
		if err == nil {
			grp.EntriesRead = g.EntriesRead
		}
	}
	return s, nil
}

func dumpVector(vs *vector.VectorSet) vectorDump {
	d := vectorDump{Dim: vs.Dimension()}
	vs.ForEach(func(id string, item *vector.VectorItem) bool {
		meta := make(map[string]string, len(item.Metadata))
		for k, v := range item.Metadata {
			meta[k] = v
		}
		d.Items = append(d.Items, vectorItemDump{ID: id, Data: item.Vector.ToFloat64(), Metadata: meta})
		return true
	})
	return d
}

func loadVector(raw []byte) (*vector.VectorSet, error) {
	var d vectorDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	vs := vector.NewVectorSet()
	for _, it := range d.Items {
		vs.Add(it.ID, vector.NewVectorFromFloat64(it.Data), it.Metadata)
	}
	return vs, nil
}

func dumpTimeSeries(ts *timeseries.TimeSeries) tsDump {
	d := tsDump{
		Key:       ts.Key,
		Retention: int64(ts.GetRetention()),
		Labels:    ts.GetLabels(),
	}
	for _, s := range ts.Range(math.MinInt64, math.MaxInt64) {
		d.Samples = append(d.Samples, tsSampleDump{Timestamp: s.Timestamp, Value: s.Value})
	}
	return d
}

func loadTimeSeries(raw []byte) (*timeseries.TimeSeries, error) {
	var d tsDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	ts := timeseries.NewTimeSeries(d.Key, time.Duration(d.Retention))
	if len(d.Labels) > 0 {
		ts.SetLabels(d.Labels)
	}
	for _, s := range d.Samples {
		if _, err := ts.Add(s.Timestamp, s.Value); err != nil {
			return nil, err
		}
	}
	return ts, nil
}

func dumpExpireDict(ed *dict.ExpireDict) expireDictDump {
	d := expireDictDump{
		Fields: make(map[string][]byte),
		Expire: make(map[string]int64),
	}
	ed.ForEach(func(field string, val interface{}) bool {
		b, ok := val.([]byte)
		if !ok {
			return true
		}
		d.Fields[field] = b
		if exp, has := ed.GetExpireTime(field); has {
			d.Expire[field] = exp.UnixMilli()
		}
		return true
	})
	if len(d.Expire) == 0 {
		d.Expire = nil
	}
	return d
}

func loadExpireDict(raw []byte) (*dict.ExpireDict, error) {
	var d expireDictDump
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	ed := dict.NewExpireDict(16)
	for field, val := range d.Fields {
		ed.Put(field, val)
	}
	now := time.Now()
	for field, ms := range d.Expire {
		exp := time.UnixMilli(ms)
		if exp.After(now) {
			ed.Expire(field, exp)
		} else {
			// already expired at dump time — drop field
			ed.Delete(field)
		}
	}
	return ed, nil
}

var godisRestoreCmd = []byte("godis.restore")

// opaqueToCmd emits GODIS.RESTORE for Godis-specific types.
func opaqueToCmd(key string, entity *database.DataEntity) *protocol.MultiBulkReply {
	payload, ok := EncodeOpaque(entity)
	if !ok {
		return nil
	}
	return protocol.MakeMultiBulkReply([][]byte{godisRestoreCmd, []byte(key), payload})
}
