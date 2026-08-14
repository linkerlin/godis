package database

import (
	"sort"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

const (
	redisFlagWrite         = "write"
	redisFlagReadonly      = "readonly"
	redisFlagDenyOOM       = "denyoom"
	redisFlagAdmin         = "admin"
	redisFlagPubSub        = "pubsub"
	redisFlagNoScript      = "noscript"
	redisFlagRandom        = "random"
	redisFlagSortForScript = "sortforscript"
	redisFlagLoading       = "loading"
	redisFlagStale         = "stale"
	redisFlagSkipMonitor   = "skip_monitor"
	redisFlagAsking        = "asking"
	redisFlagFast          = "fast"
	redisFlagMovableKeys   = "movablekeys"
	redisFlagBlocking      = "blocking"
)

func execCommand(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return getAllGodisCommandReply()
	}
	subCommand := strings.ToLower(string(args[0]))
	if subCommand == "info" {
		return getCommands(args[1:])
	} else if subCommand == "docs" {
		return getCommandDocs(args[1:])
	} else if subCommand == "count" {
		return protocol.MakeIntReply(int64(len(cmdTable)))
	} else if subCommand == "list" {
		return getCommandList(args[1:])
	} else if subCommand == "getkeys" {
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'command|" + subCommand + "'")
		}
		return getKeys(args[1:])
	} else if subCommand == "getkeysandflags" {
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'command|" + subCommand + "'")
		}
		return getKeysAndFlags(args[1:])
	} else if subCommand == "help" {
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'command|help' command")
		}
		return protocol.MakeMultiBulkReply([][]byte{
			[]byte("COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
			[]byte("(no subcommand)"),
			[]byte("    Return details about all Redis commands."),
			[]byte("COUNT"),
			[]byte("    Return the total number of commands in this Redis server."),
			[]byte("LIST"),
			[]byte("    Return a list of command names."),
			[]byte("INFO [<command-name> ...]"),
			[]byte("    Return details about multiple Redis commands."),
			[]byte("DOCS [<command-name> ...]"),
			[]byte("    Return documentary information about multiple Redis commands."),
			[]byte("GETKEYS <full-command>"),
			[]byte("    Return the keys from a full Redis command."),
			[]byte("GETKEYSANDFLAGS <full-command>"),
			[]byte("    Return the keys and access flags from a full Redis command."),
			[]byte("HELP"),
			[]byte("    Print this help."),
		})
	} else {
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCommand + "'. Try COMMAND HELP.")
	}
}

func execCommandAsDB(db *DB, args [][]byte) redis.Reply {
	_ = db
	return execCommand(args)
}

// getCommandDocs returns command documentation.
// RESP3: Map of command-name → docs Map; RESP2: flattened array via MapReply.ToBytes.
func getCommandDocs(args [][]byte) redis.Reply {
	outer := protocol.MakeMapReply()
	if len(args) == 0 {
		for name, cmd := range cmdTable {
			outer.Put(name, cmd.toDocsReply())
		}
		return outer
	}
	for _, v := range args {
		cmdName := strings.ToLower(string(v))
		if cmd, ok := cmdTable[cmdName]; ok {
			outer.Put(cmdName, cmd.toDocsReply())
		} else {
			// Unknown command: empty map (Redis still lists the name with empty docs).
			outer.Put(cmdName, protocol.MakeMapReply())
		}
	}
	return outer
}

func getKeys(args [][]byte) redis.Reply {
	cmdLine, cmdName, ok := ResolveCommandLine(args)
	if !ok {
		return protocol.MakeErrReply("ERR Invalid command specified")
	}
	cmd := cmdTable[cmdName]
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeErrReply("ERR Invalid number of arguments specified for command")
	}
	if err := validateEvalKeyArgs(cmdName, cmdLine); err != nil {
		return err
	}

	if cmd.prepare == nil {
		return protocol.MakeErrReply("ERR The command has no key arguments")
	}
	writeKeys, readKeys := cmd.prepare(cmdLine[1:])
	keys := append(writeKeys, readKeys...)
	if len(keys) == 0 && (cmd.extra == nil || cmd.extra.firstKey == 0) {
		return protocol.MakeErrReply("ERR The command has no key arguments")
	}
	resp := make([][]byte, len(keys))
	for i, key := range keys {
		resp[i] = []byte(key)
	}
	return protocol.MakeMultiBulkReply(resp)
}

// validateEvalKeyArgs checks EVAL/EVALSHA numkeys vs remaining args for COMMAND GETKEYS*.
func validateEvalKeyArgs(cmdName string, cmdLine [][]byte) redis.Reply {
	if cmdName != "eval" && cmdName != "evalsha" {
		return nil
	}
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	numKeys, err := strconv.Atoi(string(cmdLine[2]))
	if err != nil || numKeys < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if len(cmdLine) < 3+numKeys {
		return protocol.MakeErrReply("ERR Number of keys can't be greater than number of args")
	}
	return nil
}

// getKeysAndFlags implements COMMAND GETKEYSANDFLAGS.
func getKeysAndFlags(args [][]byte) redis.Reply {
	cmdLine, cmdName, ok := ResolveCommandLine(args)
	if !ok {
		return protocol.MakeErrReply("ERR Invalid command specified")
	}
	cmd := cmdTable[cmdName]
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeErrReply("ERR Invalid number of arguments specified for command")
	}
	if err := validateEvalKeyArgs(cmdName, cmdLine); err != nil {
		return err
	}
	if cmd.prepare == nil {
		return protocol.MakeErrReply("ERR The command has no key arguments")
	}
	writeKeys, readKeys := cmd.prepare(cmdLine[1:])
	writeSet := make(map[string]bool, len(writeKeys))
	for _, k := range writeKeys {
		writeSet[k] = true
	}
	readSet := make(map[string]bool, len(readKeys))
	for _, k := range readKeys {
		readSet[k] = true
	}
	seen := make(map[string]bool)
	ordered := make([]string, 0, len(writeKeys)+len(readKeys))
	for _, k := range append(append([]string{}, writeKeys...), readKeys...) {
		if seen[k] {
			continue
		}
		seen[k] = true
		ordered = append(ordered, k)
	}
	if len(ordered) == 0 && (cmd.extra == nil || cmd.extra.firstKey == 0) {
		return protocol.MakeErrReply("ERR The command has no key arguments")
	}
	replies := make([]redis.Reply, 0, len(ordered))
	for _, k := range ordered {
		flag := "R"
		if writeSet[k] && readSet[k] {
			flag = "RW"
		} else if writeSet[k] {
			flag = "W"
		}
		replies = append(replies, protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeBulkReply([]byte(k)),
			protocol.MakeMultiBulkReply([][]byte{[]byte(flag)}),
		}))
	}
	return protocol.MakeMultiRawReply(replies)
}

func getCommandList(args [][]byte) redis.Reply {
	pattern := "*"
	aclCat := ""
	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		if opt == "FILTERBY" {
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			kind := strings.ToUpper(string(args[i+1]))
			switch kind {
			case "PATTERN":
				pattern = string(args[i+2])
			case "MODULE":
				return protocol.MakeEmptyMultiBulkReply()
			case "ACLCAT":
				aclCat = strings.ToLower(string(args[i+2]))
				if !strings.HasPrefix(aclCat, "@") {
					aclCat = "@" + aclCat
				}
			default:
				return protocol.MakeSyntaxErrReply()
			}
			i += 2
			continue
		}
		return protocol.MakeSyntaxErrReply()
	}

	allowed := map[string]bool{}
	if aclCat != "" {
		cmds := commandsForACLCategory(aclCat)
		if cmds == nil {
			return protocol.MakeEmptyMultiBulkReply()
		}
		for _, c := range cmds {
			allowed[strings.ToLower(strings.TrimSpace(c))] = true
		}
	}

	match, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return protocol.MakeErrReply("ERR Invalid argument")
	}
	names := make([]string, 0, len(cmdTable))
	for name := range cmdTable {
		if aclCat != "" && !allowed[name] {
			continue
		}
		if pattern == "*" || match.IsMatch(name) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([][]byte, len(names))
	for i, n := range names {
		out[i] = []byte(n)
	}
	return protocol.MakeMultiBulkReply(out)
}

func getCommands(args [][]byte) redis.Reply {
	replies := make([]redis.Reply, len(args))
	for i, v := range args {
		cmd, ok := cmdTable[strings.ToLower(string(v))]
		if ok {
			replies[i] = cmd.toDescReply()
		} else {
			replies[i] = protocol.MakeNullBulkReply()
		}
	}
	return protocol.MakeMultiRawReply(replies)
}

func getAllGodisCommandReply() redis.Reply {
	replies := make([]redis.Reply, 0, len(cmdTable))
	for _, v := range cmdTable {
		replies = append(replies, v.toDescReply())
	}
	return protocol.MakeMultiRawReply(replies)
}

func init() {
	registerCommand("Command", execCommandAsDB, noPrepare, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Keys", 2, 0).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagSortForScript}, 0, 0, 0)
	registerSpecialCommand("Auth", 2, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagSkipMonitor, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Info", -1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("SlaveOf", 3, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("ReplicaOf", 3, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Subscribe", -2, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Publish", 3, 0).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("FlushAll", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("FlushDB", -1, 0).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("Save", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("BgSave", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("LastSave", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Select", 2, 0).
		attachCommandExtra([]string{redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("ReplConf", -1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	//attachCommandExtra("ReplConf", 3, []string{redisFlagReadonly, redisFlagAdmin, redisFlagNoScript}, 0, 0, 0, nil)

	// transaction command
	registerSpecialCommand("Multi", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Discard", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Exec", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagSkipMonitor}, 0, 0, 0)
	registerSpecialCommand("Watch", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 1, -1, 1)
	registerSpecialCommand("UnWatch", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerSpecialCommand("Reset", 1, 0).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
}
