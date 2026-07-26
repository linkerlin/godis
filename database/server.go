package database

import (
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/memory"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/protocol"
)

var godisVersion = "8.0.0"

// Server is a redis-server with full capabilities including multiple database, rdb loader, replication
type Server struct {
	dbSet []*atomic.Value // *DB

	// handle publish/subscribe
	hub *pubsub.Hub
	// handle aof persistence
	persister *aof.Persister

	// for replication
	role         int32
	slaveStatus  *slaveStatus
	masterStatus *masterStatus

	// hooks
	insertCallback database.KeyEventCallback
	deleteCallback database.KeyEventCallback

	// slow log record
	slogLogger *SlowLogger

	// client pause state
	clientPauseMu   sync.Mutex
	clientPaused    bool
	clientPauseEnd  time.Time
	clientPauseMode string // "WRITE" or "ALL"

	// initialization error if any
	initErr error

	// lock manager for advanced lock control
	lockManager *dict.LockManager

	// memory limiter for maxmemory management
	memLimiter *memory.Limiter

	// last successful SAVE/BGSAVE unix time (LASTSAVE)
	lastSaveUnix  atomic.Int64
	dirty         atomic.Int64 // changes since last successful SAVE/BGSAVE (PE-4)
	bgsaveMu      sync.Mutex
	bgsaveRunning bool
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

// NewStandaloneServer creates a standalone redis server, with multi database and all other functions
func NewStandaloneServer() (*Server, error) {
	return newServerWithSize(dataDictSize)
}

// NewTestServer creates a server with smaller dict sizes for testing
func NewTestServer() (*Server, error) {
	return newServerWithSize(testDictSize)
}

// newServerWithSize creates a server with custom dict size
func newServerWithSize(dictSize int) (*Server, error) {
	server := &Server{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}

	// create tmp dir
	err := os.MkdirAll(config.GetTmpDir(), os.ModePerm)
	if err != nil {
		return nil, errors.Wrap(err, "create tmp dir failed")
	}

	// make db set
	server.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range server.dbSet {
		singleDB := makeDBWithSize(dictSize)
		singleDB.index = i
		singleDB.server = server
		holder := &atomic.Value{}
		holder.Store(singleDB)
		server.dbSet[i] = holder
	}
	server.hub = pubsub.MakeHub()

	// record aof
	validAof := false
	if config.Properties.AppendOnly {
		validAof = fileExists(config.Properties.AppendFilename)
		aofHandler, err := NewPersister(server,
			config.Properties.AppendFilename, true, config.Properties.AppendFsync)
		if err != nil {
			return nil, errors.Wrap(err, "create persister failed")
		}
		server.bindPersister(aofHandler)
	}
	if config.Properties.RDBFilename != "" && !validAof {
		// load rdb
		err := server.loadRdbFile()
		if err != nil {
			logger.Errorf("load rdb file failed: %+v", err)
		}
	}
	server.slaveStatus = initReplSlaveStatus()
	server.initMasterStatus()
	server.lastSaveUnix.Store(time.Now().Unix())
	server.startReplCron()
	server.role = masterRole // The initialization process does not require atomicity

	// record slow log
	if config.Properties.SlowLogMaxLen <= 0 {
		config.Properties.SlowLogMaxLen = 128
	}
	if config.Properties.AclLogMaxLen <= 0 {
		config.Properties.AclLogMaxLen = 128
	}
	server.slogLogger = NewSlowLogger(config.Properties.SlowLogMaxLen, config.Properties.SlowLogSlowerThan)

	// initialize lock manager
	server.lockManager = dict.NewLockManager(nil, nil)

	// initialize memory limiter from config
	policy := config.Properties.MaxmemoryPolicy
	if policy == "" {
		policy = "noeviction"
	}
	server.memLimiter = memory.NewLimiter(&memory.Config{
		MaxMemory: config.Properties.Maxmemory,
		Policy:    policy,
	})
	server.memLimiter.SetMemUsageFunc(server.approxKeyMemoryUsage)
	server.memLimiter.Start()

	SetSlowLogLenProvider(func() int { return server.SlowLogLen() })

	// propagate lock manager to all DBs
	for _, holder := range server.dbSet {
		db := holder.Load().(*DB)
		db.SetLockManager(server.lockManager)
	}

	// initialize and propagate eviction manager
	defaultPolicy := memory.ParseEvictionPolicy(policy)
	for _, holder := range server.dbSet {
		db := holder.Load().(*DB)
		em := NewEvictionManager(db, defaultPolicy)
		db.SetEvictionManager(em)
	}

	server.InitACLEngine()

	return server, nil
}

// MustNewStandaloneServer creates a standalone server, panics on error (for backward compatibility)
func MustNewStandaloneServer() *Server {
	server, err := NewStandaloneServer()
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to create server: %+v", err))
	}
	return server
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (server *Server) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic in Exec: %v\n%s", r, string(debug.Stack()))
			result = &protocol.UnknownErrReply{}
		}
		if result != nil && protocol.IsErrorReply(result) {
			atomic.AddUint64(&serverStats.TotalErrorReplies, 1)
		}
	}()

	if len(cmdLine) == 0 {
		return protocol.MakeErrReply("ERR unknown command")
	}
	if reply := validateCmdArgCount(cmdLine); reply != nil {
		return reply
	}

	// Record the start time of command execution
	GodisExecCommandStartUnixTime := time.Now()
	if c != nil {
		if t, ok := c.(interface{ TouchActive() }); ok {
			t.TouchActive()
		}
	}

	cmdName := strings.ToLower(string(cmdLine[0]))
	if c != nil {
		if lc, ok := c.(interface{ SetLastCommand(string) }); ok {
			lc.SetLastCommand(cmdName)
		}
	}
	if reply := checkProtectedMode(c); reply != nil {
		return reply
	}
	// ping
	if cmdName == "ping" {
		return Ping(c, cmdLine[1:])
	}
	// authenticate
	if cmdName == "auth" {
		return Auth(c, cmdLine[1:])
	}
	if cmdName == "hello" {
		roleName := "master"
		if atomic.LoadInt32(&server.role) == slaveRole {
			roleName = "slave"
		}
		return HelloWithRole(c, cmdLine[1:], roleName)
	}
	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	if reply := checkACLPermission(c, cmdName, cmdLine[1:]); reply != nil {
		return reply
	}

	// CLIENT PAUSE: stall until pause expires (exempt admin handshake cmds).
	if !isClientPauseExempt(cmdName) {
		isWrite := !isReadOnlyCommand(cmdName)
		for server.CheckClientPause(isWrite) {
			time.Sleep(5 * time.Millisecond)
		}
	}

	// Pub/Sub subscribed state: only allow a small command set (Redis PS-3 / PS-5).
	if c != nil && c.SubsCount() > 0 {
		switch cmdName {
		case "subscribe", "unsubscribe", "psubscribe", "punsubscribe",
			"ssubscribe", "sunsubscribe",
			"ping", "quit", "reset":
			// allowed
		default:
			return protocol.MakeErrReply(
				"ERR Can't execute '" + strings.ToUpper(cmdName) +
					"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
			)
		}
	}

	// MONITOR stream (other clients); skip auth handshake noise.
	if cmdName != "auth" && cmdName != "hello" && cmdName != "monitor" {
		BroadcastMonitor(cmdName, cmdLine[1:], c)
	}

	// info
	if cmdName == "info" {
		return Info(server, cmdLine[1:])
	}

	// slowlog
	if cmdName == "slowlog" {
		return server.slogLogger.HandleSlowlogCommand(cmdLine)
	}

	if cmdName == "dbsize" {
		return DbSize(c, server)
	}
	if cmdName == "slaveof" || cmdName == "replicaof" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("ERR cannot use SLAVEOF/REPLICAOF within MULTI")
		}
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply(strings.ToUpper(cmdName))
		}
		return server.execSlaveOf(c, cmdLine[1:])
	} else if cmdName == "command" {
		return execCommand(cmdLine[1:])
	} else if cmdName == "config" {
		return server.execConfig(cmdLine[1:])
	} else if cmdName == "memory" {
		return execMemory(server, c, cmdLine[1:])
	} else if cmdName == "latency" {
		return execLatency(cmdLine[1:])
	} else if cmdName == "module" {
		return execModule(cmdLine[1:])
	} else if cmdName == "time" {
		return execTime(cmdLine[1:])
	} else if cmdName == "role" {
		return server.execRole(cmdLine[1:])
	} else if cmdName == "lolwut" {
		return execLolwut(cmdLine[1:])
	} else if cmdName == "pubsub" {
		return execPubsub(server.hub, cmdLine[1:])
	}

	// read only slave
	role := atomic.LoadInt32(&server.role)
	if role == slaveRole && !c.IsMaster() {
		readOnly := true
		if config.Properties != nil {
			readOnly = config.Properties.ReplicaReadOnly
		}
		if readOnly && !isReadOnlyCommand(cmdName) {
			return protocol.MakeErrReply("READONLY You can't write against a read only slave.")
		}
	}

	// special commands which cannot execute within transaction
	if cmdName == "subscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("subscribe")
		}
		return pubsub.Subscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "psubscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("psubscribe")
		}
		return pubsub.PSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "publish" {
		return pubsub.Publish(server.hub, cmdLine[1:])
	} else if cmdName == "unsubscribe" {
		return pubsub.UnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "punsubscribe" {
		return pubsub.PUnSubscribe(server.hub, c, cmdLine[1:])
	} else if cmdName == "ssubscribe" {
		if len(cmdLine) < 2 {
			return protocol.MakeArgNumErrReply("ssubscribe")
		}
		return execSSubscribeConn(c, cmdLine[1:])
	} else if cmdName == "sunsubscribe" {
		return execSUnsubscribeConn(c, cmdLine[1:])
	} else if cmdName == "schannels" {
		return execSChannels(cmdLine[1:])
	} else if cmdName == "shutdown" {
		return execShutdown(server, cmdLine[1:])
	} else if cmdName == "failover" {
		return execFailover(server, cmdLine[1:])
	} else if cmdName == "bgrewriteaof" {
		return BGRewriteAOF(server, cmdLine[1:])
	} else if cmdName == "rewriteaof" {
		return RewriteAOF(server, cmdLine[1:])
	} else if cmdName == "flushall" {
		if len(cmdLine) > 2 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if len(cmdLine) == 2 {
			opt := strings.ToUpper(string(cmdLine[1]))
			if opt != "ASYNC" && opt != "SYNC" {
				return protocol.MakeSyntaxErrReply()
			}
		}
		return server.flushAll()
	} else if cmdName == "flushdb" {
		if len(cmdLine) > 2 {
			return protocol.MakeArgNumErrReply(cmdName)
		}
		if len(cmdLine) == 2 {
			opt := strings.ToUpper(string(cmdLine[1]))
			if opt != "ASYNC" && opt != "SYNC" {
				return protocol.MakeSyntaxErrReply()
			}
		}
		if c.InMultiState() {
			return protocol.MakeErrReply("ERR command 'FlushDB' cannot be used in MULTI")
		}
		return server.execFlushDB(c.GetDBIndex())
	} else if cmdName == "save" {
		return SaveRDB(server, cmdLine[1:])
	} else if cmdName == "bgsave" {
		return BGSaveRDB(server, cmdLine[1:])
	} else if cmdName == "lastsave" {
		return execLastSave(server, cmdLine[1:])
	} else if cmdName == "wait" {
		return server.execWait(cmdLine[1:])
	} else if cmdName == "select" {
		if c != nil && c.InMultiState() {
			return protocol.MakeErrReply("ERR cannot select database within MULTI")
		}
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		dbIndex, err := strconv.Atoi(string(cmdLine[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR invalid DB index")
		}
		if dbIndex >= len(server.dbSet) || dbIndex < 0 {
			return protocol.MakeErrReply("ERR DB index is out of range")
		}
		c.SelectDB(dbIndex)
		return protocol.MakeOkReply()
	} else if cmdName == "swapdb" {
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("swapdb")
		}
		index1, err := strconv.Atoi(string(cmdLine[1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		index2, err := strconv.Atoi(string(cmdLine[2]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if index1 < 0 || index1 >= len(server.dbSet) || index2 < 0 || index2 >= len(server.dbSet) {
			return protocol.MakeErrReply("ERR DB index is out of range")
		}
		server.dbSet[index1], server.dbSet[index2] = server.dbSet[index2], server.dbSet[index1]
		currentDB := c.GetDBIndex()
		if currentDB == index1 {
			c.SelectDB(index2)
		} else if currentDB == index2 {
			c.SelectDB(index1)
		}
		server.AddAof(c.GetDBIndex(), utils.ToCmdLine3("swapdb", cmdLine[1:]...))
		return protocol.MakeOkReply()
	} else if cmdName == "copy" {
		if len(cmdLine) < 3 {
			return protocol.MakeArgNumErrReply("copy")
		}
		return execCopy(server, c, cmdLine[1:])
	} else if cmdName == "move" {
		if len(cmdLine) != 3 {
			return protocol.MakeArgNumErrReply("move")
		}
		return server.execMove(c, cmdLine[1:])
	} else if cmdName == "replconf" {
		return server.execReplConf(c, cmdLine[1:])
	} else if cmdName == "psync" {
		return server.execPSync(c, cmdLine[1:])
	}
	// todo: support multi database transaction

	// normal commands
	dbIndex := c.GetDBIndex()
	selectedDB, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}

	exec := selectedDB.Exec(c, cmdLine)
	// Record slow query logs
	server.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.Name())
	return exec
}

// AfterClientClose does some clean after client close connection
func (server *Server) AfterClientClose(c redis.Connection) {
	if id := c.GetTrackingID(); id != "" {
		DisableTracking(id)
		c.SetTrackingID("")
	}
	RemoveMonitorClient(c)
	pubsub.UnsubscribeAll(server.hub, c)
	shardedHub.AfterClientClose(c)
}

// Close graceful shutdown database
func (server *Server) Close() {
	// stop slaveStatus first
	if server.slaveStatus != nil {
		server.slaveStatus.close()
	}
	if server.persister != nil {
		server.persister.Close()
	}
	server.stopMaster()

	// stop memory limiter
	if server.memLimiter != nil {
		server.memLimiter.Stop()
	}
}

// GetLockManager returns the lock manager
func (server *Server) GetLockManager() *dict.LockManager {
	return server.lockManager
}

// GetMemLimiter returns the memory limiter
func (server *Server) GetMemLimiter() *memory.Limiter {
	return server.memLimiter
}

// SlowLogLen returns the number of entries in the slowlog.
func (server *Server) SlowLogLen() int {
	if server.slogLogger == nil {
		return 0
	}
	return server.slogLogger.Len()
}

// CheckClientPause checks if client processing should be paused
func (server *Server) CheckClientPause(isWrite bool) bool {
	server.clientPauseMu.Lock()
	defer server.clientPauseMu.Unlock()
	if !server.clientPaused {
		return false
	}
	if time.Now().After(server.clientPauseEnd) {
		server.clientPaused = false
		return false
	}
	if server.clientPauseMode == "ALL" {
		return true
	}
	return isWrite
}

func isClientPauseExempt(cmdName string) bool {
	switch cmdName {
	case "auth", "hello", "ping", "quit", "reset", "client", "info":
		return true
	default:
		return false
	}
}

// setClientPause configures client pause (used by CLIENT PAUSE).
func (server *Server) setClientPause(timeoutMs int, mode string) {
	server.clientPauseMu.Lock()
	defer server.clientPauseMu.Unlock()
	server.clientPaused = true
	server.clientPauseEnd = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	server.clientPauseMode = mode
}

// clearClientPause clears client pause (CLIENT UNPAUSE).
func (server *Server) clearClientPause() {
	server.clientPauseMu.Lock()
	defer server.clientPauseMu.Unlock()
	server.clientPaused = false
}

func (server *Server) execFlushDB(dbIndex int) redis.Reply {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, utils.ToCmdLine("FlushDB"))
	}
	return server.flushDB(dbIndex)
}

// flushDB flushes the selected database
func (server *Server) flushDB(dbIndex int) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	newDB := makeDB()
	return server.loadDB(dbIndex, newDB)
}

func (server *Server) loadDB(dbIndex int, newDB *DB) redis.Reply {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	oldDB, err := server.selectDBSafe(dbIndex)
	if err != nil {
		logger.Errorf("loadDB failed: %+v", err)
		return protocol.MakeErrReply("ERR internal error")
	}
	newDB.index = dbIndex
	newDB.addAof = oldDB.addAof // inherit oldDB
	server.dbSet[dbIndex].Store(newDB)
	return &protocol.OkReply{}
}

// flushAll flushes all databases.
func (server *Server) flushAll() redis.Reply {
	for i := range server.dbSet {
		server.flushDB(i)
	}
	if server.persister != nil {
		server.persister.SaveCmdLine(0, utils.ToCmdLine("FlushAll"))
	}
	return &protocol.OkReply{}
}

// selectDB returns the database with the given index, or an error if the index is out of range.
func (server *Server) selectDB(dbIndex int) (*DB, *protocol.StandardErrReply) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, protocol.MakeErrReply("ERR DB index is out of range")
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// selectDBSafe returns the database safely with error handling
func (server *Server) selectDBSafe(dbIndex int) (*DB, error) {
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return nil, errors.Newf("DB index %d is out of range", dbIndex)
	}
	return server.dbSet[dbIndex].Load().(*DB), nil
}

// ForEach traverses all the keys in the given database
func (server *Server) ForEach(dbIndex int, cb func(key string, data *database.DataEntity, expiration *time.Time) bool) error {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return err
	}
	db.ForEach(cb)
	return nil
}

// GetEntity returns the data entity to the given key
func (server *Server) GetEntity(dbIndex int, key string) (*database.DataEntity, bool, error) {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return nil, false, err
	}
	entity, ok := db.GetEntity(key)
	return entity, ok, nil
}

func (server *Server) GetExpiration(dbIndex int, key string) (*time.Time, error) {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return nil, err
	}
	raw, ok := db.ttlMap.Get(key)
	if !ok {
		return nil, nil
	}
	expireTime, _ := raw.(time.Time)
	return &expireTime, nil
}

// ExecMulti executes multi commands transaction Atomically and Isolated
func (server *Server) ExecMulti(conn redis.Connection, watching map[string]uint64, cmdLines []CmdLine) redis.Reply {
	selectedDB, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return selectedDB.ExecMulti(conn, watching, cmdLines)
}

// RWLocks lock keys for writing and reading
func (server *Server) RWLocks(dbIndex int, writeKeys []string, readKeys []string) error {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return err
	}
	db.RWLocks(writeKeys, readKeys)
	return nil
}

// RWUnLocks unlock keys for writing and reading
func (server *Server) RWUnLocks(dbIndex int, writeKeys []string, readKeys []string) error {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return err
	}
	db.RWUnLocks(writeKeys, readKeys)
	return nil
}

// GetUndoLogs return rollback commands
func (server *Server) GetUndoLogs(dbIndex int, cmdLine [][]byte) ([]CmdLine, error) {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return nil, err
	}
	return db.GetUndoLogs(cmdLine), nil
}

// ExecWithLock executes normal commands, invoker should provide locks
func (server *Server) ExecWithLock(conn redis.Connection, cmdLine [][]byte) redis.Reply {
	db, errReply := server.selectDB(conn.GetDBIndex())
	if errReply != nil {
		return errReply
	}
	return db.execWithLock(conn, cmdLine)
}

// BGRewriteAOF asynchronously rewrites Append-Only-File
func BGRewriteAOF(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		// Redis allows the command when AOF is off (no-op success status)
		return protocol.MakeStatusReply("Background append only file rewriting started")
	}
	if err := db.persister.RunRewriteAsync(); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeStatusReply("Background append only file rewriting started")
}

// RewriteAOF start Append-Only-File rewriting and blocked until it finished
func RewriteAOF(db *Server, args [][]byte) redis.Reply {
	if db.persister == nil {
		return protocol.MakeOkReply()
	}
	err := db.persister.Rewrite()
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeOkReply()
}

// SaveRDB start RDB writing and blocked until it finished
func SaveRDB(db *Server, args [][]byte) redis.Reply {
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	var err error
	if db.persister != nil {
		err = db.persister.GenerateRDB(rdbFilename)
	} else {
		err = aof.WriteRDBFromDB(rdbFilename, db)
	}
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	db.resetDirtyAfterSave()
	return protocol.MakeOkReply()
}

// BGSaveRDB asynchronously save RDB
func BGSaveRDB(db *Server, args [][]byte) redis.Reply {
	rdbFilename := config.Properties.RDBFilename
	if rdbFilename == "" {
		rdbFilename = "dump.rdb"
	}
	if db.persister != nil {
		if err := db.persister.RunGenerateRDBAsync(rdbFilename, func(err error) {
			if err == nil {
				db.resetDirtyAfterSave()
			}
		}); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		return protocol.MakeStatusReply("Background saving started")
	}
	db.bgsaveMu.Lock()
	if db.bgsaveRunning {
		db.bgsaveMu.Unlock()
		return protocol.MakeErrReply("ERR Background save already in progress")
	}
	db.bgsaveRunning = true
	db.bgsaveMu.Unlock()
	go func() {
		defer func() {
			db.bgsaveMu.Lock()
			db.bgsaveRunning = false
			db.bgsaveMu.Unlock()
		}()
		if err := aof.WriteRDBFromDB(rdbFilename, db); err != nil {
			logger.Warn("BGSAVE failed: " + err.Error())
			return
		}
		db.resetDirtyAfterSave()
	}()
	return protocol.MakeStatusReply("Background saving started")
}

// execLastSave returns the unix time of the last successful SAVE/BGSAVE
func execLastSave(server *Server, args [][]byte) redis.Reply {
	return protocol.MakeIntReply(server.lastSaveUnix.Load())
}

// GetDBSize returns keys count and ttl key count
func (server *Server) GetDBSize(dbIndex int) (int, int, error) {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return 0, 0, err
	}
	return db.data.Len(), db.ttlMap.Len(), nil
}

func (server *Server) startReplCron() {
	go func(mdb *Server) {
		replTicker := time.NewTicker(time.Second * 10)
		saveTicker := time.NewTicker(time.Second)
		defer replTicker.Stop()
		defer saveTicker.Stop()
		for {
			select {
			case <-replTicker.C:
				mdb.slaveCron()
				mdb.masterCron()
			case <-saveTicker.C:
				mdb.checkSavePoints()
			}
		}
	}(server)
}

// GetAvgTTL calculates the average remaining TTL in milliseconds for keys that
// have an expiration (Redis INFO keyspace avg_ttl semantics).
func (server *Server) GetAvgTTL(dbIndex, randomKeyCount int) (int64, error) {
	var ttlSum int64
	var withExpire int64
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return 0, err
	}
	keys := db.data.RandomKeys(randomKeyCount)
	now := time.Now()
	for _, k := range keys {
		rawExpireTime, ok := db.ttlMap.Get(k)
		if !ok {
			continue
		}
		expireTime, _ := rawExpireTime.(time.Time)
		remain := expireTime.Sub(now).Milliseconds()
		if remain > 0 {
			ttlSum += remain
			withExpire++
		}
	}
	if withExpire == 0 {
		return 0, nil
	}
	return ttlSum / withExpire, nil
}

// CountSubexpiry counts hash fields that have a per-field TTL (INFO keyspace subexpiry).
func (server *Server) CountSubexpiry(dbIndex int) (int, error) {
	db, err := server.selectDBSafe(dbIndex)
	if err != nil {
		return 0, err
	}
	n := 0
	db.data.ForEach(func(_ string, raw interface{}) bool {
		entity, ok := raw.(*database.DataEntity)
		if !ok || entity == nil {
			return true
		}
		if ed, ok := entity.Data.(*dict.ExpireDict); ok {
			n += ed.ExpireFieldCount()
		}
		return true
	})
	return n, nil
}

func (server *Server) SetKeyInsertedCallback(cb database.KeyEventCallback) {
	server.insertCallback = cb
	for i := range server.dbSet {
		db, err := server.selectDBSafe(i)
		if err != nil {
			logger.Errorf("SetKeyInsertedCallback failed for db %d: %+v", i, err)
			continue
		}
		db.insertCallback = cb
	}
}

func (server *Server) SetKeyDeletedCallback(cb database.KeyEventCallback) {
	server.deleteCallback = cb
	for i := range server.dbSet {
		db, err := server.selectDBSafe(i)
		if err != nil {
			logger.Errorf("SetKeyDeletedCallback failed for db %d: %+v", i, err)
			continue
		}
		db.deleteCallback = cb
	}
}
