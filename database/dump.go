package database

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/rdb/crc64jones"
	rdbenc "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/linkerlin/godis/aof"
	Dict "github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/datastruct/hll"
	List "github.com/linkerlin/godis/datastruct/list"
	HashSet "github.com/linkerlin/godis/datastruct/set"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// Redis DUMP footer: 2-byte RDB version (LE) + 8-byte CRC64-Jones (LE).
// Version 11 matches REDIS0011 used by hdt3213/rdb encoder.
const dumpRDBVersion = uint16(11)

// execDump serializes the value stored at key in Redis DUMP format
// (RDB object + version + CRC64). TTL is not embedded; use RESTORE's ttl arg.
func execDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'dump' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	payload, err := encodeDumpPayload(entity)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeBulkReply(payload)
}

// execRestore deserializes a DUMP payload into key
// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
func execRestore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'restore' command")
	}

	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	serializedData := args[2]
	replace := false
	absTTL := false
	var idleSec int64 = -1
	var freqVal int64 = -1

	for i := 3; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "REPLACE":
			replace = true
		case "ABSTTL":
			absTTL = true
		case "IDLETIME":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR Invalid IDLETIME value, must be >= 0")
			}
			idleSec = n
			i++
		case "FREQ":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR Invalid FREQ value, must be >= 0")
			}
			freqVal = n
			i++
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	_, exists := db.GetEntity(key)
	if exists && !replace {
		return protocol.MakeErrReply("BUSYKEY Target key name already exists.")
	}

	entity, err := decodeDumpPayload(serializedData)
	if err != nil {
		return protocol.MakeErrReply("ERR DUMP payload version or checksum are wrong")
	}

	db.PutEntity(key, entity)

	if ttlArg > 0 {
		var expireTime time.Time
		if absTTL {
			expireTime = time.Unix(0, ttlArg*int64(time.Millisecond))
		} else {
			expireTime = time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
		}
		db.Expire(key, expireTime)
	} else {
		db.Persist(key)
	}

	if db.evictionManager != nil {
		if idleSec >= 0 {
			db.evictionManager.SeedIdle(key, idleSec)
		}
		if freqVal >= 0 {
			db.evictionManager.SeedFreq(key, uint64(freqVal))
		}
	}

	db.addAof(utils.ToCmdLine3("restore", args...))
	// RESTORE may replace a different-typed value at key; drop stale index
	// entries then reindex the restored content (no-op for non-indexed types).
	removeKeyFromIndex(db, key)
	reindexKey(db, key)
	return protocol.MakeOkReply()
}

func execRestoreAsking(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'restore-asking' command")
	}
	newArgs := append(args, []byte("REPLACE"))
	return execRestore(db, newArgs)
}

func encodeDumpPayload(entity *database.DataEntity) ([]byte, error) {
	full := &bytes.Buffer{}
	enc := rdbenc.NewEncoder(full)
	if err := enc.WriteHeader(); err != nil {
		return nil, err
	}
	if err := enc.WriteDBHeader(0, 1, 0); err != nil {
		return nil, err
	}
	start := full.Len()
	if err := writeEntityToRDB(enc, "", entity); err != nil {
		return nil, err
	}
	withKey := full.Bytes()[start:]
	// WriteXxxObject encodes: type + key + value. Empty key is always 1-byte length 0.
	if len(withKey) < 2 || withKey[1] != 0x00 {
		return nil, errDumpUnsupported("unexpected RDB key encoding")
	}
	obj := append([]byte{withKey[0]}, withKey[2:]...)
	return appendDumpFooter(obj), nil
}

func decodeDumpPayload(payload []byte) (*database.DataEntity, error) {
	if len(payload) < 10 {
		return nil, errDumpBadPayload
	}
	bodyLen := len(payload) - 10
	body := payload[:bodyLen]
	ver := binary.LittleEndian.Uint16(payload[bodyLen : bodyLen+2])
	crcGot := binary.LittleEndian.Uint64(payload[bodyLen+2:])
	if ver == 0 || ver > 12 {
		return nil, errDumpBadPayload
	}
	if crcGot != 0 {
		h := crc64jones.New()
		_, _ = h.Write(payload[:bodyLen+2])
		if h.Sum64() != crcGot {
			return nil, errDumpBadPayload
		}
	}
	if len(body) < 1 {
		return nil, errDumpBadPayload
	}

	// Wrap DUMP object as a single-key RDB (empty key) for the existing decoder.
	var rdbBuf bytes.Buffer
	rdbBuf.WriteString("REDIS0011")
	rdbBuf.WriteByte(0xFE) // SELECTDB
	rdbBuf.WriteByte(0x00)
	rdbBuf.WriteByte(0xFB) // RESIZEDB
	rdbBuf.WriteByte(0x01)
	rdbBuf.WriteByte(0x00)
	rdbBuf.WriteByte(body[0]) // type
	rdbBuf.WriteByte(0x00)    // empty key
	rdbBuf.Write(body[1:])
	rdbBuf.WriteByte(0xFF)                // EOF
	rdbBuf.Write(make([]byte, 8))         // ignored file CRC
	rdbBuf.WriteByte(0x0a)

	var entity *database.DataEntity
	dec := rdb.NewDecoder(bytes.NewReader(rdbBuf.Bytes()))
	err := dec.Parse(func(o rdb.RedisObject) bool {
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			if restored, ok := aof.DecodeOpaque(str.Value); ok {
				entity = restored
			} else if restored, ok := decodeDumpExtraOpaque(str.Value); ok {
				entity = restored
			} else {
				entity = &database.DataEntity{Data: str.Value}
			}
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := List.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &database.DataEntity{Data: list}
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := Dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{Data: hash}
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := HashSet.Make()
			for _, mem := range setObj.Members {
				set.Add(string(mem))
			}
			entity = &database.DataEntity{Data: set}
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zset := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zset.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{Data: zset}
		}
		return true
	})
	if err != nil || entity == nil {
		return nil, errDumpBadPayload
	}
	return entity, nil
}

func appendDumpFooter(obj []byte) []byte {
	out := make([]byte, len(obj)+10)
	copy(out, obj)
	binary.LittleEndian.PutUint16(out[len(obj):], dumpRDBVersion)
	h := crc64jones.New()
	_, _ = h.Write(out[:len(obj)+2])
	binary.LittleEndian.PutUint64(out[len(obj)+2:], h.Sum64())
	return out
}

// cloneDataEntity returns an independent copy of entity (for COPY/MOVE).
func cloneDataEntity(entity *database.DataEntity) (*database.DataEntity, error) {
	if entity == nil {
		return nil, errDumpUnsupported("nil entity")
	}
	payload, err := encodeDumpPayload(entity)
	if err != nil {
		return nil, err
	}
	return decodeDumpPayload(payload)
}

func writeEntityToRDB(enc *rdbenc.Encoder, key string, entity *database.DataEntity) error {
	switch val := entity.Data.(type) {
	case []byte:
		return enc.WriteStringObject(key, val)
	case List.List:
		vals := make([][]byte, 0, val.Len())
		val.ForEach(func(_ int, v interface{}) bool {
			b, _ := v.([]byte)
			vals = append(vals, b)
			return true
		})
		return enc.WriteListObject(key, vals)
	case *HashSet.Set:
		vals := make([][]byte, 0, val.Len())
		val.ForEach(func(m string) bool {
			vals = append(vals, []byte(m))
			return true
		})
		return enc.WriteSetObject(key, vals)
	case *SortedSet.SortedSet:
		entries := make([]*model.ZSetEntry, 0, val.Len())
		elements := val.RangeByRank(0, val.Len(), false)
		for _, e := range elements {
			entries = append(entries, &model.ZSetEntry{Member: e.Member, Score: e.Score})
		}
		return enc.WriteZSetObject(key, entries)
	case *Dict.ExpireDict:
		payload, ok := aof.EncodeOpaque(entity)
		if !ok {
			return errDumpUnsupported("ExpireDict opaque encode failed")
		}
		return enc.WriteStringObject(key, payload)
	case Dict.Dict:
		hash := make(map[string][]byte)
		val.ForEach(func(field string, v interface{}) bool {
			b, ok := v.([]byte)
			if ok {
				hash[field] = b
			}
			return true
		})
		return enc.WriteHashMapObject(key, hash)
	default:
		// stream / JSON / vector / timeseries / bloom → Godis opaque string (not Redis-wire)
		payload, ok := aof.EncodeOpaque(entity)
		if !ok {
			return errDumpUnsupported("DUMP not implemented for this data type")
		}
		return enc.WriteStringObject(key, payload)
	}
}

// HLL lives in package database; encode with the same GODIS1 envelope aof uses.
var dumpOpaqueMagic = []byte("GODIS1\x00")

type dumpOpaqueEnv struct {
	Type string          `json:"t"`
	Data json.RawMessage `json:"d"`
}

// decodeDumpExtraOpaque restores legacy godis HLL dump payloads (a JSON
// register array) into the modern string-stored dense HLL format, so old dump
// files remain loadable.
func decodeDumpExtraOpaque(payload []byte) (*database.DataEntity, bool) {
	if !aof.IsOpaquePayload(payload) {
		return nil, false
	}
	var env dumpOpaqueEnv
	if err := json.Unmarshal(payload[len(dumpOpaqueMagic):], &env); err != nil {
		return nil, false
	}
	if env.Type != "hll" {
		return nil, false
	}
	var regs []uint8
	if err := json.Unmarshal(env.Data, &regs); err != nil || len(regs) != hll.Registers {
		return nil, false
	}
	h := hll.New()
	copy(h.Registers(), regs)
	return &database.DataEntity{Data: h.Encode()}, true
}

type dumpError string

func (e dumpError) Error() string { return string(e) }

var (
	errDumpBadPayload = dumpError("DUMP payload version or checksum are wrong")
)

func errDumpUnsupported(msg string) error { return dumpError(msg) }

func init() {
	registerCommand("Dump", execDump, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("Restore", execRestore, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
}
