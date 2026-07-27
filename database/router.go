package database

import (
	"strings"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var cmdTable = make(map[string]*command)

var (
	cmdAliasesOnce sync.Once
	cmdAliases     map[string]string
)

// buildCommandAliases creates mappings from multi-word command names (as standard
// Redis clients send them) to the single-token names used in cmdTable.
func buildCommandAliases() {
	cmdAliases = make(map[string]string)
	for name := range cmdTable {
		lower := strings.ToLower(name)
		var parts []string
		if strings.Contains(lower, ".") {
			parts = strings.SplitN(lower, ".", 2)
		} else if strings.Contains(lower, "|") {
			parts = strings.SplitN(lower, "|", 2)
		}
		if len(parts) == 2 {
			alias := parts[0] + " " + parts[1]
			cmdAliases[alias] = lower
		}
	}

	// Godis-specific commands without a separator in their registration name.
	manual := [][]string{
		{"vs", "add", "vsadd"},
		{"vs", "get", "vsget"},
		{"vs", "del", "vsdel"},
		{"vs", "search", "vssearch"},
		{"vs", "query", "vsquery"},
		{"vs", "range", "vsrange"},
		{"vs", "len", "vslen"},
		{"vs", "card", "vscard"},
		{"tdigest", "create", "tdigest.create"},
		{"tdigest", "add", "tdigest.add"},
		{"tdigest", "quantile", "tdigest.quantile"},
		{"tdigest", "cdf", "tdigest.cdf"},
		{"tdigest", "info", "tdigest.info"},
	}
	for _, m := range manual {
		cmdAliases[m[0]+" "+m[1]] = m[2]
	}
}

// ResolveCommandLine resolves a possibly multi-word command into the internal
// command name and a reconstructed command line where cmdLine[0] is the internal
// name. It returns the original cmdLine unchanged when the first token already
// maps to a registered command.
func ResolveCommandLine(cmdLine [][]byte) (newCmdLine [][]byte, cmdName string, ok bool) {
	if len(cmdLine) == 0 {
		return nil, "", false
	}
	cmdAliasesOnce.Do(buildCommandAliases)

	cmdName = strings.ToLower(string(cmdLine[0]))
	if _, exists := cmdTable[cmdName]; exists {
		return cmdLine, cmdName, true
	}

	if len(cmdLine) >= 2 {
		alias := cmdName + " " + strings.ToLower(string(cmdLine[1]))
		if full, found := cmdAliases[alias]; found {
			newCmdLine = make([][]byte, 0, len(cmdLine)-1)
			newCmdLine = append(newCmdLine, []byte(full))
			newCmdLine = append(newCmdLine, cmdLine[2:]...)
			return newCmdLine, full, true
		}
	}

	return nil, cmdName, false
}

type command struct {
	name     string
	executor ExecFunc
	// prepare returns related keys command
	prepare PreFunc
	// undo generates undo-log before command actually executed, in case the command needs to be rolled back
	undo UndoFunc
	// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
	// for example: the arity of `get` is 2, `mget` is -2
	arity int
	flags int
	extra *commandExtra
}

type commandExtra struct {
	signs    []string
	firstKey int
	lastKey  int
	keyStep  int
}

const flagWrite = 0

const (
	flagReadOnly = 1 << iota
	flagSpecial  // command invoked in Exec
	flagAdmin    // admin command
	flagFast     // fast command
	flagLoading  // command allowed during loading
)

// registerCommand registers a normal command, which only read or modify a limited number of keys
func registerCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int, flags int) *command {
	name = strings.ToLower(name)
	cmd := &command{
		name:     name,
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
		flags:    flags,
	}
	cmdTable[name] = cmd
	return cmd
}

// registerSpecialCommand registers a special command, such as publish, select, keys, flushAll
func registerSpecialCommand(name string, arity int, flags int) *command {
	name = strings.ToLower(name)
	flags |= flagSpecial
	cmd := &command{
		name:  name,
		arity: arity,
		flags: flags,
	}
	cmdTable[name] = cmd
	return cmd
}

func isReadOnlyCommand(name string) bool {
	name = strings.ToLower(name)
	cmd := cmdTable[name]
	if cmd == nil {
		return false
	}
	return cmd.flags&flagReadOnly > 0
}

func (cmd *command) toDescReply() redis.Reply {
	args := make([]redis.Reply, 0, 6)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(cmd.extra.firstKey)),
			protocol.MakeIntReply(int64(cmd.extra.lastKey)),
			protocol.MakeIntReply(int64(cmd.extra.keyStep)),
		)
	}
	return protocol.MakeMultiRawReply(args)
}

func (cmd *command) toDocsReply() redis.Reply {
	// COMMAND DOCS reply format (flat array of field/value pairs).
	var result [][]byte

	result = append(result, []byte("summary"))
	result = append(result, []byte("Command "+cmd.name))
	result = append(result, []byte("since"))
	result = append(result, []byte("6.0.0"))
	result = append(result, []byte("group"))
	result = append(result, []byte(cmd.docsGroup()))
	result = append(result, []byte("complexity"))
	result = append(result, []byte("O(1)"))
	result = append(result, []byte("doc_flags"))
	result = append(result, protocol.MakeEmptyMultiBulkReply().ToBytes())

	return protocol.MakeMultiBulkReply(result)
}

func (cmd *command) docsGroup() string {
	if g, ok := knownDocsGroups[cmd.name]; ok {
		return g
	}
	if cmd.extra != nil {
		for _, s := range cmd.extra.signs {
			switch s {
			case redisFlagPubSub:
				return "pubsub"
			case redisFlagAdmin:
				return "server"
			}
		}
	}
	return "generic"
}

// knownDocsGroups maps common command names to Redis COMMAND DOCS groups.
var knownDocsGroups = map[string]string{
	"get": "string", "set": "string", "setex": "string", "psetex": "string", "setnx": "string",
	"mget": "string", "mset": "string", "incr": "string", "decr": "string", "append": "string", "strlen": "string",
	"hget": "hash", "hset": "hash", "hgetall": "hash", "hdel": "hash", "hkeys": "hash", "hvals": "hash",
	"lpush": "list", "rpush": "list", "lrange": "list", "llen": "list", "lpop": "list", "rpop": "list",
	"sadd": "set", "smembers": "set", "srem": "set", "scard": "set",
	"zadd": "sorted_set", "zrange": "sorted_set", "zscore": "sorted_set", "zcard": "sorted_set",
	"xadd": "stream", "xread": "stream", "xrange": "stream",
	"eval": "scripting", "evalsha": "scripting",
	"auth": "connection", "ping": "connection", "hello": "connection", "select": "connection", "echo": "connection",
	"info": "server", "config": "server", "client": "server", "memory": "server", "acl": "server",
	"multi": "transactions", "exec": "transactions", "discard": "transactions", "watch": "transactions",
	"subscribe": "pubsub", "publish": "pubsub", "psubscribe": "pubsub", "pubsub": "pubsub",
	"del": "generic", "expire": "generic", "ttl": "generic", "keys": "generic", "type": "generic", "exists": "generic",
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}
