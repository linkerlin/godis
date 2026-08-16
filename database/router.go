package database

import (
	"strings"
	"sync"

	"github.com/linkerlin/godis/acl"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var cmdTable = make(map[string]*command)

var (
	cmdAliasesOnce sync.Once
	cmdAliases     map[string]string
)

// arityErrCmdName returns the command name Redis uses in wrong-arity ERRs.
// Vector Set (V*) and RediSearch (FT.*) use uppercase; JSON/BF/TS stay lowercase.
func arityErrCmdName(cmdName string) string {
	switch cmdName {
	case "vadd", "vsim", "vrem", "vcard", "vdim", "vemb", "vinfo",
		"vismember", "vrandmember", "vsetattr", "vgetattr", "vlinks", "vrange":
		return strings.ToUpper(cmdName)
	default:
		if strings.HasPrefix(cmdName, "ft.") {
			return strings.ToUpper(cmdName)
		}
		return cmdName
	}
}

// unknownParentSubcommandReply matches Redis when a multi-word parent (SCRIPT/
// FUNCTION) is sent with a missing or unknown subcommand that is not registered
// via Script|*/Function|* aliases.
func unknownParentSubcommandReply(cmdLine [][]byte) redis.Reply {
	if len(cmdLine) == 0 {
		return nil
	}
	parent := strings.ToLower(string(cmdLine[0]))
	help := ""
	switch parent {
	case "script":
		help = "SCRIPT"
	case "function":
		help = "FUNCTION"
	default:
		return nil
	}
	if len(cmdLine) == 1 {
		return protocol.MakeArgNumErrReply(parent)
	}
	return protocol.MakeErrReply("ERR unknown subcommand '" + string(cmdLine[1]) + "'. Try " + help + " HELP.")
}

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

	return cmdLine, cmdName, false
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
	// Redis 7+ COMMAND INFO shape: name, arity, flags, first_key, last_key,
	// step, acl_categories, tips, key_specs, subcommands.
	args := make([]redis.Reply, 0, 10)
	args = append(args,
		protocol.MakeBulkReply([]byte(cmd.name)),
		protocol.MakeIntReply(int64(cmd.arity)))
	firstKey, lastKey, keyStep := 0, 0, 0
	if cmd.extra != nil {
		signs := make([][]byte, len(cmd.extra.signs))
		for i, v := range cmd.extra.signs {
			signs[i] = []byte(v)
		}
		firstKey, lastKey, keyStep = cmd.extra.firstKey, cmd.extra.lastKey, cmd.extra.keyStep
		args = append(args,
			protocol.MakeMultiBulkReply(signs),
			protocol.MakeIntReply(int64(firstKey)),
			protocol.MakeIntReply(int64(lastKey)),
			protocol.MakeIntReply(int64(keyStep)),
		)
	} else {
		args = append(args,
			protocol.MakeMultiBulkReply([][]byte{}),
			protocol.MakeIntReply(0),
			protocol.MakeIntReply(0),
			protocol.MakeIntReply(0),
		)
	}
	// acl_categories, tips, key_specs, subcommands.
	args = append(args, protocol.MakeMultiRawReply(cmd.aclCategoryReplies()))
	args = append(args, protocol.MakeMultiRawReply([]redis.Reply{})) // tips
	args = append(args, protocol.MakeMultiRawReply(cmd.keySpecReplies(firstKey, lastKey, keyStep)))
	args = append(args, protocol.MakeMultiRawReply([]redis.Reply{})) // subcommands
	return protocol.MakeMultiRawReply(args)
}

// aclCategoryReplies returns the command's ACL categories as bulk replies,
// sourced from the acl package's category table (Redis 8 COMMAND INFO/DOCS).
func (cmd *command) aclCategoryReplies() []redis.Reply {
	cats := acl.GetCommandCategories(cmd.name)
	out := make([]redis.Reply, 0, len(cats))
	for _, c := range cats {
		out = append(out, protocol.MakeBulkReply([]byte(c)))
	}
	return out
}

// keySpecReplies renders a minimal Redis 7+ key_specs entry derived from the
// registered first/last/step metadata. Returns an empty list when the command
// declares no keys.
func (cmd *command) keySpecReplies(firstKey, lastKey, keyStep int) []redis.Reply {
	if firstKey <= 0 {
		return []redis.Reply{}
	}
	if keyStep <= 0 {
		keyStep = 1
	}
	// One key spec: flags (RO/RW), begin_search index, find_keys range.
	spec := make([]redis.Reply, 0, 6)
	spec = append(spec, protocol.MakeBulkReply([]byte("flags")))
	flags := make([]redis.Reply, 0, 1)
	if cmd.flags&flagReadOnly > 0 {
		flags = append(flags, protocol.MakeBulkReply([]byte("RO")))
	} else {
		flags = append(flags, protocol.MakeBulkReply([]byte("RW")))
	}
	spec = append(spec, protocol.MakeMultiRawReply(flags))
	spec = append(spec, protocol.MakeBulkReply([]byte("begin_search")))
	spec = append(spec, protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("type")),
		protocol.MakeBulkReply([]byte("index")),
		protocol.MakeBulkReply([]byte("pos")),
		protocol.MakeIntReply(int64(firstKey)),
	}))
	spec = append(spec, protocol.MakeBulkReply([]byte("find_keys")))
	spec = append(spec, protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("type")),
		protocol.MakeBulkReply([]byte("range")),
		protocol.MakeBulkReply([]byte("lastkey")),
		protocol.MakeIntReply(int64(lastKey)),
		protocol.MakeBulkReply([]byte("step")),
		protocol.MakeIntReply(int64(keyStep)),
		protocol.MakeBulkReply([]byte("limit")),
		protocol.MakeIntReply(0),
	}))
	return []redis.Reply{protocol.MakeMultiRawReply(spec)}
}

func (cmd *command) toDocsReply() redis.Reply {
	// Per-command docs Map (summary/group/…); nested arrays are true nests.
	m := protocol.MakeMapReply()
	m.Put("summary", protocol.MakeBulkReply([]byte("Command "+cmd.name)))
	m.Put("since", protocol.MakeBulkReply([]byte("6.0.0")))
	m.Put("group", protocol.MakeBulkReply([]byte(cmd.docsGroup())))
	m.Put("complexity", protocol.MakeBulkReply([]byte("O(1)")))
	m.Put("doc_flags", protocol.MakeMultiBulkReply(cmd.docFlags()))
	m.Put("acl_categories", protocol.MakeMultiRawReply(cmd.aclCategoryReplies()))
	m.Put("key_specs", protocol.MakeMultiRawReply(cmd.keySpecReplies(cmd.firstLastStep())))
	return m
}

// firstLastStep returns the command's first/last/step key metadata (0s when
// unset) for key_specs rendering.
func (cmd *command) firstLastStep() (int, int, int) {
	if cmd.extra == nil {
		return 0, 0, 0
	}
	return cmd.extra.firstKey, cmd.extra.lastKey, cmd.extra.keyStep
}

// docFlags returns Redis COMMAND DOCS documentary flags (e.g. syscmd).
func (cmd *command) docFlags() [][]byte {
	if cmd.extra == nil {
		return [][]byte{}
	}
	out := make([][]byte, 0, 1)
	for _, s := range cmd.extra.signs {
		if s == redisFlagAdmin {
			out = append(out, []byte("syscmd"))
			break
		}
	}
	return out
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
