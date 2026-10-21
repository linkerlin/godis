package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// Some commands are implemented in Server.Exec (connection / multi-DB /
// replication layer) rather than as regular DB executors. They still need
// COMMAND metadata so clients can discover them, and so replica read-only
// checks treat them correctly. The executor is never reached on the normal
// Server.Exec path; it only guards direct DB.Exec use.
func execServerOnlyCommand(db *DB, args [][]byte) redis.Reply {
	return protocol.MakeErrReply("ERR command is handled by the server connection layer")
}

func execFunctionParentMeta(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'function' command")
	}
	return protocol.MakeErrReply("ERR unknown subcommand '" + string(args[0]) + "'. Try FUNCTION HELP.")
}

func execScriptParentMeta(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'script' command")
	}
	return protocol.MakeErrReply("ERR unknown subcommand '" + string(args[0]) + "'. Try SCRIPT HELP.")
}

func prepareCopyMeta(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	source, dest := string(args[0]), string(args[1])
	if source == dest {
		return []string{source}, nil
	}
	return []string{dest}, []string{source}
}

func copyKeyOrder(args [][]byte) []string {
	if len(args) < 2 {
		return nil
	}
	if string(args[0]) == string(args[1]) {
		return []string{string(args[0])}
	}
	return []string{string(args[0]), string(args[1])}
}

func prepareMoveMeta(args [][]byte) ([]string, []string) {
	if len(args) == 0 {
		return nil, nil
	}
	return []string{string(args[0])}, nil
}

func init() {
	registerCommand("BgRewriteAof", execServerOnlyCommand, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	copyCmd := registerCommand("Copy", execServerOnlyCommand, prepareCopyMeta, nil, -3, flagWrite)
	copyCmd.keyOrder = copyKeyOrder
	copyCmd.attachCommandExtra([]string{redisFlagWrite, redisFlagMovableKeys}, 1, 2, 1)
	registerCommand("DBSize", execServerOnlyCommand, nil, nil, 1, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagFast}, 0, 0, 0)
	registerCommand("Function", execFunctionParentMeta, nil, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerCommand("HImport", execServerOnlyCommand, nil, nil, -2, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagNoScript}, 0, 0, 0)
	registerCommand("Move", execServerOnlyCommand, prepareMoveMeta, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("PSubscribe", execServerOnlyCommand, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerCommand("PSync", execServerOnlyCommand, nil, nil, 3, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerCommand("PUnsubscribe", execServerOnlyCommand, nil, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerCommand("Script", execScriptParentMeta, nil, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerCommand("Slowlog", execServerOnlyCommand, nil, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerCommand("SwapDB", execServerOnlyCommand, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 0, 0, 0)
	registerCommand("Sync", execServerOnlyCommand, nil, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
	registerCommand("Unsubscribe", execServerOnlyCommand, nil, nil, -1, flagReadOnly).
		attachCommandExtra([]string{redisFlagPubSub, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerCommand("Wait", execServerOnlyCommand, nil, nil, 3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagNoScript}, 0, 0, 0)
	registerCommand("WaitAOF", execServerOnlyCommand, nil, nil, 4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagNoScript}, 0, 0, 0)
}
