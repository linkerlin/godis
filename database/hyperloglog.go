package database

import (
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// HyperLogLog implementation using Redis's HLL algorithm
// This is a simplified version using 16KB per HLL (2^14 registers)

const (
	hllRegisters     = 16384 // 2^14
	hllBits          = 14
	hllRegistersMask = hllRegisters - 1
)

// HLL represents a HyperLogLog data structure
type HLL struct {
	registers []uint8
}

// NewHLL creates a new HyperLogLog
func NewHLL() *HLL {
	return &HLL{
		registers: make([]uint8, hllRegisters),
	}
}

// Add adds an element to the HLL. Returns true if any register was updated.
func (h *HLL) Add(elem []byte) bool {
	hash := hashBytes(elem)
	index := hash & hllRegistersMask
	// Count leading zeros + 1
	value := uint8(bits.LeadingZeros64(hash>>hllBits)) + 1
	if value > h.registers[index] {
		h.registers[index] = value
		return true
	}
	return false
}

// Count returns the estimated cardinality
func (h *HLL) Count() uint64 {
	var sum float64
	var emptyRegisters int

	for _, val := range h.registers {
		sum += 1.0 / math.Pow(2.0, float64(val))
		if val == 0 {
			emptyRegisters++
		}
	}

	// HLL estimator
	alpha := 0.7213 / (1.0 + 1.079/float64(hllRegisters))
	estimate := alpha * float64(hllRegisters*hllRegisters) / sum

	// Small range correction
	if estimate <= 2.5*float64(hllRegisters) && emptyRegisters != 0 {
		return uint64(float64(hllRegisters) * math.Log(float64(hllRegisters)/float64(emptyRegisters)))
	}

	// Large range correction not implemented for simplicity
	return uint64(estimate)
}

// Merge merges another HLL into this one
func (h *HLL) Merge(other *HLL) {
	for i := 0; i < hllRegisters; i++ {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// Simple hash function
func hashBytes(data []byte) uint64 {
	var hash uint64 = 14695981039346656037 // FNV offset basis
	for _, b := range data {
		hash ^= uint64(b)
		hash *= 1099511628211 // FNV prime
	}
	return hash
}

// execPFAdd adds elements to a HyperLogLog
// PFADD key element [element ...]
func execPFAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfadd' command")
	}

	key := string(args[0])
	hll, errReply := db.getAsHLL(key)
	if errReply != nil {
		return errReply
	}

	isNew := false
	if hll == nil {
		hll = NewHLL()
		isNew = true
	}

	added := false
	for i := 1; i < len(args); i++ {
		if hll.Add(args[i]) {
			added = true
		}
	}

	if isNew {
		db.PutEntity(key, &database.DataEntity{Data: hll})
	}

	if added {
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
		key := string(args[0])
		hll, errReply := db.getAsHLL(key)
		if errReply != nil {
			return errReply
		}
		if hll == nil {
			return protocol.MakeIntReply(0)
		}
		return protocol.MakeIntReply(int64(hll.Count()))
	}

	// Multi-key case: merge and count
	mergedHLL := NewHLL()
	for _, arg := range args {
		key := string(arg)
		hll, errReply := db.getAsHLL(key)
		if errReply != nil {
			return errReply
		}
		if hll != nil {
			mergedHLL.Merge(hll)
		}
	}

	return protocol.MakeIntReply(int64(mergedHLL.Count()))
}

// execPFMerge merges multiple HyperLogLogs
// PFMERGE destkey [sourcekey ...]
func execPFMerge(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfmerge' command")
	}

	destKey := string(args[0])
	mergedHLL := NewHLL()

	for i := 1; i < len(args); i++ {
		sourceKey := string(args[i])
		hll, errReply := db.getAsHLL(sourceKey)
		if errReply != nil {
			return errReply
		}
		if hll != nil {
			mergedHLL.Merge(hll)
		}
	}

	db.PutEntity(destKey, &database.DataEntity{Data: mergedHLL})
	db.addAof(utils.ToCmdLine3("pfmerge", args...))
	return protocol.MakeOkReply()
}

// getAsHLL gets a HyperLogLog from database
func (db *DB) getAsHLL(key string) (*HLL, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	hll, ok := entity.Data.(*HLL)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return hll, nil
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

// execPFDebug handles PFDEBUG subcommands (GETREG for now).
// PFDEBUG GETREG key
func execPFDebug(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pfdebug' command")
	}
	sub := strings.ToUpper(string(args[0]))
	key := string(args[1])
	switch sub {
	case "GETREG":
		entity, exists := db.GetEntity(key)
		if !exists {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		hll, ok := entity.Data.(*HLL)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		regs := make([][]byte, len(hll.registers))
		for i, v := range hll.registers {
			regs[i] = []byte(strconv.Itoa(int(v)))
		}
		return protocol.MakeMultiBulkReply(regs)
	case "DECODE":
		entity, exists := db.GetEntity(key)
		if !exists {
			return protocol.MakeErrReply("ERR key does not exist")
		}
		hll, ok := entity.Data.(*HLL)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		// Simplified dense encoding summary (godis HLL is always dense registers).
		msg := fmt.Sprintf("encoding:dense registers:%d", len(hll.registers))
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
