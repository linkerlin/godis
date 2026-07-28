// Package database is a memory database with redis compatible interface
package database

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/timewheel"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/protocol"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
)

// testDictSize is used for testing to reduce memory consumption
const testDictSize = 16

// DB stores data and execute user's commands
type DB struct {
	index int
	// key -> DataEntity
	data *dict.ConcurrentDict
	// key -> expireTime (time.Time)
	ttlMap *dict.ConcurrentDict
	// key -> version(uint32)
	versionMap *dict.ConcurrentDict

	// addaof is used to add command to aof
	addAof func(CmdLine)

	// callbacks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback

	// lockManager for advanced lock control (optional)
	lockManager *dict.LockManager

	// evictionManager for memory limit eviction (optional)
	evictionManager *EvictionManager

	// parent server (nil in isolated DB unit tests)
	server *Server
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) redis.Reply

// PreFunc analyses command line when queued command to `multi`
// returns related write keys and read keys
type PreFunc func(args [][]byte) ([]string, []string)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// UndoFunc returns undo logs for the given command line
// execute from head to tail when undo
type UndoFunc func(db *DB, args [][]byte) []CmdLine

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeDBWithSize creates DB instance with custom dict sizes
func makeDBWithSize(dictSize int) *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dictSize),
		ttlMap:     dict.MakeConcurrent(dictSize),
		versionMap: dict.MakeConcurrent(dictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// makeBasicDB create DB instance only with basic abilities.
func makeBasicDB() *DB {
	db := &DB{
		data:       dict.MakeConcurrent(dataDictSize),
		ttlMap:     dict.MakeConcurrent(ttlDictSize),
		versionMap: dict.MakeConcurrent(dataDictSize),
		addAof:     func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
func (db *DB) Exec(c redis.Connection, cmdLine [][]byte) redis.Reply {
	if len(cmdLine) == 0 {
		return protocol.MakeErrReply("ERR unknown command")
	}
	if reply := validateCmdArgCount(cmdLine); reply != nil {
		return reply
	}
	// transaction control commands and other commands which cannot execute within transaction
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "multi" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return StartMulti(c)
	} else if cmdName == "discard" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return DiscardMulti(c)
	} else if cmdName == "exec" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execMulti(db, c)
	} else if cmdName == "watch" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("ERR WATCH inside MULTI is not allowed")
		}
		if !validateArity(-2, cmdLine) {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return Watch(db, c, cmdLine[1:])
	} else if cmdName == "unwatch" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		// Redis queues UNWATCH inside MULTI (unlike WATCH which is rejected).
		if c != nil && c.InMultiState() {
			c.EnqueueCmd(cmdLine)
			return protocol.MakeQueuedReply()
		}
		return UnWatch(c)
	} else if cmdName == "reset" {
		if len(cmdLine) != 1 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		return execReset(c, db)
	}
	if c != nil && c.InMultiState() {
		return EnqueueCmd(c, cmdLine)
	}

	if cmdName == "client" {
		return execClientConn(c, db, cmdLine[1:])
	}
	if cmdName == "acl" {
		return execACLConn(c, db, cmdLine[1:])
	}
	if cmdName == "monitor" {
		if c != nil {
			AddMonitorClient(c)
		}
		return protocol.MakeOkReply()
	}

	// ponytail: handle pub/sub commands that may be called from Lua scripts
	if cmdName == "publish" {
		if db.server != nil {
			return pubsub.Publish(db.server.hub, cmdLine[1:])
		}
		return protocol.MakeIntReply(0)
	}
	if cmdName == "subscribe" || cmdName == "unsubscribe" ||
		cmdName == "psubscribe" || cmdName == "punsubscribe" {
		return protocol.MakeErrReply("ERR " + cmdName + " is not allowed from Lua scripts")
	}

	return db.execNormalCommand(c, cmdLine)
}

func (db *DB) execNormalCommand(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdLine, cmdName, ok := ResolveCommandLine(cmdLine)
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	cmd := cmdTable[cmdName]
	if reply := validateCmdArgCount(cmdLine); reply != nil {
		return reply
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}

	start := time.Now()
	prepare := cmd.prepare
	var write, read []string
	if prepare != nil {
		write, read = prepare(cmdLine[1:])
		if reply := validatePreparedKeyStrings(write, read); reply != nil {
			return reply
		}
	}
	// Enforce maxmemory before taking key locks (eviction may remove other keys).
	if (cmd.flags&flagReadOnly) == 0 && db.server != nil {
		if errReply := db.server.ensureMemoryForWrite(db, bytesPerKeyEstimate); errReply != nil {
			return errReply
		}
	}
	db.addVersion(write...)
	db.RWLocks(write, read)
	defer db.RWUnLocks(write, read)
	if c != nil {
		BindBlockingClientID(c.GetClientID())
		defer ClearBlockingClientID()
		if nt, ok := c.(interface{ GetNoTouch() bool }); ok && nt.GetNoTouch() {
			bindNoTouch()
			defer clearNoTouch()
		}
	}
	fun := cmd.executor
	result := fun(db, cmdLine[1:])

	// Record command stats
	usec := uint64(time.Since(start).Microseconds())
	failed := protocol.IsErrorReply(result)
	RecordCommand(cmdName, usec, failed)
	applyCacheHooks(c, cmdName, write, read, failed)
	if !failed && (cmd.flags&flagReadOnly) == 0 && db.server != nil {
		db.server.incrDirty()
	}

	return result
}

// execWithLock executes normal commands, invoker should provide locks
func (db *DB) execWithLock(c redis.Connection, cmdLine [][]byte) redis.Reply {
	cmdLine, cmdName, ok := ResolveCommandLine(cmdLine)
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	cmd := cmdTable[cmdName]
	if reply := validateCmdArgCount(cmdLine); reply != nil {
		return reply
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	if cmd.prepare != nil {
		write, read := cmd.prepare(cmdLine[1:])
		if reply := validatePreparedKeyStrings(write, read); reply != nil {
			return reply
		}
		fun := cmd.executor
		result := fun(db, cmdLine[1:])
		failed := protocol.IsErrorReply(result)
		applyCacheHooks(c, cmdName, write, read, failed)
		return result
	}
	fun := cmd.executor
	result := fun(db, cmdLine[1:])
	failed := protocol.IsErrorReply(result)
	applyCacheHooks(c, cmdName, nil, nil, failed)
	return result
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		atomic.AddUint64(&serverStats.KeyspaceMisses, 1)
		return nil, false
	}
	if db.IsExpired(key) {
		atomic.AddUint64(&serverStats.KeyspaceMisses, 1)
		return nil, false
	}
	atomic.AddUint64(&serverStats.KeyspaceHits, 1)
	entity, _ := raw.(*database.DataEntity)
	if db.evictionManager != nil && !peekNoTouch() {
		db.evictionManager.Touch(key)
	}
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	ret := db.data.PutWithLock(key, entity)
	if db.evictionManager != nil {
		db.evictionManager.Touch(key)
	}
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExistsWithLock(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	ret := db.data.PutIfAbsentWithLock(key, entity)
	// db.insertCallback may be set as nil, during `if` and actually callback
	// so introduce a local variable `cb`
	if cb := db.insertCallback; ret > 0 && cb != nil {
		cb(db.index, key, entity)
	}
	return ret
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	raw, deleted := db.data.RemoveWithLock(key)
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
	if db.evictionManager != nil {
		db.evictionManager.Forget(key)
	}
	if cb := db.deleteCallback; cb != nil {
		var entity *database.DataEntity
		if deleted > 0 {
			entity = raw.(*database.DataEntity)
		}
		cb(db.index, key, entity)
	}
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.GetWithLock(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
// deprecated
// for test only
func (db *DB) Flush() {
	db.data.Clear()
	db.ttlMap.Clear()
}

/* ---- Lock Function ----- */

// RWLocks lock keys for writing and reading
func (db *DB) RWLocks(writeKeys []string, readKeys []string) {
	db.data.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *DB) RWUnLocks(writeKeys []string, readKeys []string) {
	db.data.RWUnLocks(writeKeys, readKeys)
}

// SetLockManager sets the lock manager for advanced lock control
func (db *DB) SetLockManager(lm *dict.LockManager) {
	db.lockManager = lm
}

// RWLocksWithTimeout locks keys with timeout support
// Returns error if lock cannot be acquired within timeout
func (db *DB) RWLocksWithTimeout(writeKeys, readKeys []string, holderID string) error {
	if db.lockManager == nil {
		// Fall back to regular locks if no lock manager configured
		db.RWLocks(writeKeys, readKeys)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), db.lockManager.GetTimeout())
	defer cancel()

	return db.lockManager.RWLocksWithTimeout(ctx, writeKeys, readKeys, holderID)
}

// RWUnLocksWithTimeout unlocks keys (companion to RWLocksWithTimeout)
func (db *DB) RWUnLocksWithTimeout(writeKeys, readKeys []string, holderID string) {
	if db.lockManager == nil {
		db.RWUnLocks(writeKeys, readKeys)
		return
	}

	db.lockManager.RWUnLocks(writeKeys, readKeys, holderID)
}

// SetEvictionManager sets the eviction manager for memory limit control
func (db *DB) SetEvictionManager(em *EvictionManager) {
	db.evictionManager = em
}

// EvictIfNeeded triggers eviction if memory limit is exceeded
func (db *DB) EvictIfNeeded(maxMemory, currentUsage int64) int {
	if db.evictionManager == nil {
		return 0
	}

	if !db.evictionManager.ShouldEvict(maxMemory, currentUsage) {
		return 0
	}

	// Try to free 10% of maxmemory
	target := maxMemory / 10
	return db.evictionManager.EvictKeys(target)
}

/* ---- TTL Functions ---- */

func genExpireTask(key string) string {
	return "expire:" + key
}

// Expire sets ttlCmd of key
func (db *DB) Expire(key string, expireTime time.Time) {
	db.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		keys := []string{key}
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, ok := db.ttlMap.Get(key)
		if !ok {
			return
		}
		expireTime, _ := rawExpireTime.(time.Time)
		expired := time.Now().After(expireTime)
		if expired {
			atomic.AddUint64(&serverStats.ExpiredKeys, 1)
			db.Remove(key)
		}
	})
}

// Persist cancel ttlCmd of key
func (db *DB) Persist(key string) {
	db.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

// IsExpired check whether a key is expired
func (db *DB) IsExpired(key string) bool {
	rawExpireTime, ok := db.ttlMap.Get(key)
	if !ok {
		return false
	}
	expireTime, _ := rawExpireTime.(time.Time)
	expired := time.Now().After(expireTime)
	if expired {
		// Track stale expiration (key was accessed but already expired)
		atomic.AddUint64(&serverStats.ExpiredKeys, 1)
		serverStats.ExpiredStale++
		db.Remove(key)
	}
	return expired
}

/* --- add version --- */

func (db *DB) addVersion(keys ...string) {
	for _, key := range keys {
		versionCode := db.GetVersion(key)
		// Prevent overflow
		if versionCode == ^uint64(0) {
			versionCode = 0
		}
		db.versionMap.Put(key, versionCode+1)
	}
}

// GetVersion returns version code for given key
func (db *DB) GetVersion(key string) uint64 {
	entity, ok := db.versionMap.Get(key)
	if !ok {
		return 0
	}
	// Handle both uint32 (old data) and uint64 (new)
	switch v := entity.(type) {
	case uint32:
		return uint64(v)
	case uint64:
		return v
	default:
		return 0
	}
}

// ForEach traverses all the keys in the database
func (db *DB) ForEach(cb func(key string, data *database.DataEntity, expiration *time.Time) bool) {
	db.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		var expiration *time.Time
		rawExpireTime, ok := db.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expiration = &expireTime
		}

		return cb(key, entity, expiration)
	})
}
