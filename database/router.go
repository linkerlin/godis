package database

import (
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var cmdTable = make(map[string]*command)

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
	// COMMAND DOCS reply format:
	// 1) "summary" - short description
	// 2) "since" - Redis version
	// 3) "group" - command group
	// 4) "complexity" - time complexity
	var result [][]byte

	result = append(result, []byte("summary"))
	result = append(result, []byte("Command "+cmd.name))
	result = append(result, []byte("since"))
	result = append(result, []byte("6.0.0"))
	result = append(result, []byte("group"))
	result = append(result, []byte("generic"))
	result = append(result, []byte("complexity"))
	result = append(result, []byte("O(1)"))

	return protocol.MakeMultiBulkReply(result)
}

func (cmd *command) attachCommandExtra(signs []string, firstKey int, lastKey int, keyStep int) {
	cmd.extra = &commandExtra{
		signs:    signs,
		firstKey: firstKey,
		lastKey:  lastKey,
		keyStep:  keyStep,
	}
}
