package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/hll"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// HyperLogLog values are stored as plain strings (Redis semantics): the bytes
// are the dense HYLL encoding, so GET reads them, RDB/AOF persist them via the
// string path, and the encoding is byte-compatible with Redis HLLs.

// execPFAdd adds elements to a HyperLogLog
// PFADD key [element ...] — Redis arity -2 (key alone creates empty HLL → 1).
func execPFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfadd' command")
	}

	key := string(args[0])
	entity, exists := db.GetEntity(key)
	var h *hll.HLL
	isNew := false
	if !exists {
		h = hll.New()
		isNew = true
	} else {
		raw, ok := entity.Data.([]byte)
		if !ok {
			return protocol.MakeErrReply("WRONGTYPE Key is not a valid HyperLogLog string value.")
		}
		var err error
		h, err = hll.Decode(raw)
		if err != nil {
			if err == hll.ErrSparseEncoding || err == hll.ErrCorruptHLL {
				return protocol.MakeErrReply("ERR INVALIDOBJ Corrupted HLL object detected")
			}
			return protocol.MakeErrReply("WRONGTYPE Key is not a valid HyperLogLog string value.")
		}
	}

	added := false
	for i := 1; i < len(args); i++ {
		if h.Add(args[i]) {
			added = true
		}
	}

	// Persist when registers change, or when creating a new (possibly empty) HLL.
	if isNew || added {
		db.PutEntity(key, &database.DataEntity{Data: h.Encode()})
		db.addAof(utils.ToCmdLine3("pfadd", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execPFCount returns the approximated cardinality
// PFCOUNT key [key ...]
func execPFCount(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfcount' command")
	}

	// Single key case
	if len(args) == 1 {
		h, errReply := db.getAsHLL(string(args[0]))
		if errReply != nil {
			return errReply
		}
		if h == nil {
			return protocol.MakeIntReply(0)
		}
		return protocol.MakeIntReply(int64(h.Count()))
	}

	// Multi-key case: merge and count
	merged := hll.New()
	for _, arg := range args {
		h, errReply := db.getAsHLL(string(arg))
		if errReply != nil {
			return errReply
		}
		if h != nil {
			merged.Merge(h)
		}
	}

	return protocol.MakeIntReply(int64(merged.Count()))
}

// execPFMerge merges multiple HyperLogLogs
// PFMERGE destkey [sourcekey ...]
func execPFMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfmerge' command")
	}

	destKey := string(args[0])
	merged := hll.New()

	for i := 1; i < len(args); i++ {
		h, errReply := db.getAsHLL(string(args[i]))
		if errReply != nil {
			return errReply
		}
		if h != nil {
			merged.Merge(h)
		}
	}

	db.PutEntity(destKey, &database.DataEntity{Data: merged.Encode()})
	db.addAof(utils.ToCmdLine3("pfmerge", args...))
	return protocol.MakeOkReply()
}

// getAsHLL decodes the HLL stored under key (a string). Missing key returns
// (nil, nil); a non-HLL string is a WRONGTYPE. Sparse Redis HLLs are promoted
// to dense in memory (read path does not rewrite the key).
func (db *DB) getAsHLL(key string) (*hll.HLL, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	raw, ok := entity.Data.([]byte)
	if !ok {
		return nil, protocol.MakeErrReply("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	if !hll.IsHLLString(raw) {
		return nil, protocol.MakeErrReply("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	h, err := hll.Decode(raw)
	if err != nil {
		if err == hll.ErrSparseEncoding || err == hll.ErrCorruptHLL {
			return nil, protocol.MakeErrReply("ERR INVALIDOBJ Corrupted HLL object detected")
		}
		return nil, protocol.MakeErrReply("WRONGTYPE Key is not a valid HyperLogLog string value.")
	}
	return h, nil
}

func init() {
	registerCommand("PFAdd", execPFAdd, writeFirstKey, rollbackFirstKey, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM, redisFlagFast}, 1, 1, 1)
	registerCommand("PFCount", execPFCount, readAllKeys, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 1, 1, 1)
	registerCommand("PFMerge", execPFMerge, preparePFMerge, undoPFMerge, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, -1, 1)
	registerCommand("PFDebug", execPFDebug, preparePFDebug, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagAdmin}, 2, 1, 1)
}

func preparePFDebug(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	return nil, []string{string(args[1])}
}

// execPFDebug handles PFDEBUG subcommands (GETREG/DECODE for now).
// PFDEBUG GETREG key
func execPFDebug(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfdebug' command")
	}
	sub := strings.ToUpper(string(args[0]))
	key := string(args[1])
	switch sub {
	case "GETREG":
		h, errReply := db.getAsHLL(key)
		if errReply != nil {
			return errReply
		}
		if h == nil {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		regs := make([][]byte, hll.Registers)
		for i, v := range h.Registers() {
			regs[i] = []byte(strconv.Itoa(int(v)))
		}
		return protocol.MakeMultiBulkReply(regs)
	case "DECODE":
		h, errReply := db.getAsHLL(key)
		if errReply != nil {
			return errReply
		}
		if h == nil {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		msg := fmt.Sprintf("encoding:dense registers:%d", hll.Registers)
		return protocol.MakeBulkReply([]byte(msg))
	case "PERIOD":
		// Redis PFDEBUG PERIOD sets sparse→dense conversion threshold.
		// Godis HLL is always dense; accept and store for compatibility.
		n, err := strconv.Atoi(key) // args[1] reused as period value
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		hllDebugPeriod = n
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR Unknown PFDEBUG subcommand '" + string(args[0]) + "'")
	}
}

// hllDebugPeriod is accepted by PFDEBUG PERIOD (compat stub; dense HLL ignores it).
var hllDebugPeriod = 0

// preparePFMerge prepares keys for PFMERGE
func preparePFMerge(args [][]byte) ([]string, []string) {
	writeKeys := []string{string(args[0])}
	readKeys := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		readKeys = append(readKeys, string(args[i]))
	}
	return writeKeys, readKeys
}

// undoPFMerge generates rollback command for PFMERGE
func undoPFMerge(db *DB, args [][]byte) []CmdLine {
	// Just delete the destination key
	return []CmdLine{utils.ToCmdLine("DEL", string(args[0]))}
}
