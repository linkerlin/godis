package database

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/hdt3213/godis/interface/database"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// execDump serializes the value stored at key in a Redis-specific format
// DUMP key
func execDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'dump' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	// Serialize the value using a simple format
	// Format: [1 byte type][4 bytes TTL (0 if no TTL)][value data]
	var data []byte
	
	// Get TTL if exists
	var ttlMs uint32 = 0
	raw, hasTTL := db.ttlMap.Get(key)
	if hasTTL {
		expireTime := raw.(time.Time)
		ttlMs = uint32(time.Until(expireTime).Milliseconds())
		if ttlMs < 0 {
			ttlMs = 0
		}
	}

	// Serialize based on type
	switch val := entity.Data.(type) {
	case []byte:
		data = make([]byte, 5+len(val))
		data[0] = 0x00 // String type
		binary.BigEndian.PutUint32(data[1:5], ttlMs)
		copy(data[5:], val)
	default:
		// For other types, use string representation
		strVal := utils.ToCmdLine3("dump", args...)
		_ = strVal
		// Simplified: return error for complex types
		return protocol.MakeErrReply("ERR DUMP not fully implemented for this data type")
	}

	return protocol.MakeBulkReply(data)
}

// execRestore deserializes the value stored at key
// RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
func execRestore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'restore' command")
	}

	key := string(args[0])
	ttlArg, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	serializedData := args[2]
	replace := false
	absTTL := false

	// Parse optional arguments
	for i := 3; i < len(args); i++ {
		arg := string(args[i])
		switch arg {
		case "REPLACE":
			replace = true
		case "ABSTTL":
			absTTL = true
		case "IDLETIME", "FREQ":
			// Skip these options with their values
			if i+1 < len(args) {
				i++
			}
		}
	}

	// Check if key exists
	_, exists := db.GetEntity(key)
	if exists && !replace {
		return protocol.MakeErrReply("BUSYKEY Target key name already exists.")
	}

	// Deserialize data
	if len(serializedData) < 5 {
		return protocol.MakeErrReply("ERR DUMP payload version or checksum are wrong")
	}

	dataType := serializedData[0]
	ttlMs := binary.BigEndian.Uint32(serializedData[1:5])
	value := serializedData[5:]

	// Restore based on type
	var entity *database.DataEntity
	switch dataType {
	case 0x00: // String type
		entity = &database.DataEntity{Data: value}
	default:
		return protocol.MakeErrReply("ERR DUMP payload version or checksum are wrong")
	}

	// Store the key
	db.PutEntity(key, entity)

	// Set TTL if specified
	if ttlArg > 0 {
		var expireTime time.Time
		if absTTL {
			// ttlArg is absolute timestamp in milliseconds
			expireTime = time.Unix(0, int64(ttlArg)*int64(time.Millisecond))
		} else {
			expireTime = time.Now().Add(time.Duration(ttlArg) * time.Millisecond)
		}
		db.Expire(key, expireTime)
	} else if ttlMs > 0 && ttlArg == 0 {
		// Use TTL from dump data if no explicit TTL provided
		expireTime := time.Now().Add(time.Duration(ttlMs) * time.Millisecond)
		db.Expire(key, expireTime)
	}

	db.addAof(utils.ToCmdLine3("restore", args...))
	return protocol.MakeOkReply()
}

// execRestoreAsking is used in cluster mode to restore a key from another node
// RESTORE-ASKING key ttl serialized-value
func execRestoreAsking(db *DB, args [][]byte) redis.Reply {
	if len(args) != 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'restore-asking' command")
	}
	// For now, same as RESTORE with REPLACE
	newArgs := append(args, []byte("REPLACE"))
	return execRestore(db, newArgs)
}

func init() {
	registerCommand("Dump", execDump, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagRandom}, 1, 1, 1)
	registerCommand("Restore", execRestore, writeFirstKey, rollbackFirstKey, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
}
