package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execPing pings the server
// PING [message]
func execPing(db *DB, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeStatusReply("PONG")
	}
	if len(args) == 1 {
		return protocol.MakeBulkReply(args[0])
	}
	return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
}

// execEcho echoes the message
// ECHO message
func execEcho(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'echo' command")
	}
	return protocol.MakeBulkReply(args[0])
}

// execQuit closes the connection
// QUIT
func execQuit(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'quit' command")
	}
	// Signal connection to close
	return protocol.MakeStatusReply("OK")
}

// execClient handles CLIENT subcommands
func execClient(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "LIST":
		return execClientList(args[1:])
	case "INFO":
		return execClientInfo(args[1:])
	case "SETNAME":
		return execClientSetName(args[1:])
	case "GETNAME":
		return execClientGetName(args[1:])
	case "KILL":
		return execClientKill(args[1:])
	case "PAUSE":
		return execClientPause(db, args[1:])
	case "UNPAUSE":
		return execClientUnpause(db, args[1:])
	case "ID":
		return execClientID(args[1:])
	case "REPLY":
		return execClientReply(args[1:])
	case "TRACKING":
		return execClientTracking(args[1:])
	case "CACHING":
		return execClientCaching(args[1:])
	case "GETREDIR":
		return execClientGetRedir(args[1:])
	case "TRACKINGINFO":
		return execClientTrackingInfo(args[1:])
	case "UNBLOCK":
		return execClientUnblock(args[1:])
	case "HELP":
		return execClientHelp(args[1:])
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCmd + "'. Try CLIENT HELP.")
	}
}

// execClientList lists client connections (legacy entry without conn; prefer registry).
func execClientList(args [][]byte) redis.Reply {
	return execClientListConn(nil, args)
}

// execClientInfo returns info about current client (no conn → empty stub avoided).
func execClientInfo(args [][]byte) redis.Reply {
	return protocol.MakeErrReply("ERR CLIENT INFO requires a client connection")
}

// execClientKill kills client connections via registry filters.
func execClientKill(args [][]byte) redis.Reply {
	return execClientKillConn(nil, args)
}

// execClientSetName sets client name (registry path without conn).
func execClientSetName(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|setname' command")
	}
	return protocol.MakeErrReply("ERR CLIENT SETNAME requires a client connection")
}

// execClientGetName gets client name (registry path without conn).
func execClientGetName(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|getname' command")
	}
	return protocol.MakeErrReply("ERR CLIENT GETNAME requires a client connection")
}

// execClientPause pauses clients
// CLIENT PAUSE timeout [WRITE|ALL]
func execClientPause(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|pause' command")
	}

	timeout, err := strconv.Atoi(string(args[0]))
	if err != nil || timeout < 0 {
		return protocol.MakeErrReply("ERR timeout is not an integer or out of range")
	}

	mode := "ALL"
	if len(args) == 2 {
		mode = strings.ToUpper(string(args[1]))
		if mode != "WRITE" && mode != "ALL" {
			return protocol.MakeErrReply("ERR mode must be WRITE or ALL")
		}
	}

	// Simplified: would actually pause processing
	if db.server != nil {
		db.server.setClientPause(timeout, mode)
	}

	return protocol.MakeOkReply()
}

// execClientUnpause unpauses clients
// CLIENT UNPAUSE
func execClientUnpause(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|unpause' command")
	}

	if db.server != nil {
		db.server.clearClientPause()
	}
	return protocol.MakeOkReply()
}

// execClientID returns current client ID (registry path without conn).
func execClientID(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|id' command")
	}
	return protocol.MakeErrReply("ERR CLIENT ID requires a client connection")
}

// execClientReply controls command replies (registry path without conn).
func execClientReply(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|reply' command")
	}
	mode := strings.ToUpper(string(args[0]))
	switch mode {
	case "ON", "OFF", "SKIP":
		return protocol.MakeErrReply("ERR CLIENT REPLY requires a client connection")
	default:
		return protocol.MakeErrReply("ERR syntax error")
	}
}

// execClientTracking enables/disables client tracking
// CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix [prefix ...]] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
func execClientTracking(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|tracking' command")
	}

	mode := strings.ToUpper(string(args[0]))
	if mode != "ON" && mode != "OFF" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	// Parse options
	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "REDIRECT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			_, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR Invalid client ID")
			}
			i++
		case "PREFIX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			// prefix value
			i++
		case "BCAST", "OPTIN", "OPTOUT", "NOLOOP":
			// flags
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	return protocol.MakeStatusReply("OK")
}

// execClientCaching controls caching in OPTIN/OPTOUT mode
// CLIENT CACHING YES|NO
func execClientCaching(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|caching' command")
	}

	val := strings.ToUpper(string(args[0]))
	if val != "YES" && val != "NO" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	return protocol.MakeStatusReply("OK")
}

func execClientCachingConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|caching' command")
	}
	val := strings.ToUpper(string(args[0]))
	if val != "YES" && val != "NO" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	id := c.GetTrackingID()
	if id == "" {
		return protocol.MakeErrReply("ERR CLIENT CACHING can be issued only after CLIENT TRACKING ON")
	}
	SetCachingDirective(id, val == "YES")
	return protocol.MakeStatusReply("OK")
}

// execClientGetRedirConn returns tracking redirection target for this connection.
// CLIENT GETREDIR → -1 (off), 0 (on, no redirect), or target client id.
func execClientGetRedirConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|getredir' command")
	}
	id := c.GetTrackingID()
	if id == "" {
		return protocol.MakeIntReply(-1)
	}
	info := GetTrackingInfo(id)
	redirect, _ := info["redirect"].(string)
	if redirect == "" || redirect == "0" {
		return protocol.MakeIntReply(0)
	}
	n, err := strconv.ParseInt(redirect, 10, 64)
	if err != nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(n)
}

// execClientGetRedir is kept for legacy no-connection dispatch; prefer Conn variant.
func execClientGetRedir(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|getredir' command")
	}
	return protocol.MakeIntReply(-1)
}

// execClientUnblock unblocks a client blocked on keys
// CLIENT UNBLOCK client-id [TIMEOUT|ERROR]
func execClientUnblock(args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|unblock' command")
	}

	id, err := strconv.ParseInt(string(args[0]), 10, 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}

	mode := ""
	if len(args) == 2 {
		mode = strings.ToUpper(string(args[1]))
		if mode != "TIMEOUT" && mode != "ERROR" {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	if UnblockClientByID(id, mode) {
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeIntReply(0)
}

// execClientTrackingInfo returns tracking information (no connection context).
// CLIENT TRACKINGINFO
func execClientTrackingInfo(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|trackinginfo' command")
	}
	info := map[string]interface{}{
		"enabled":  false,
		"mode":     "",
		"prefixes": []string{},
	}
	return formatClientTrackingInfo(info)
}

// formatClientTrackingInfo builds TRACKINGINFO as a Map (RESP3 %) /
// flat field array (RESP2 via MapReply.ToBytes).
func formatClientTrackingInfo(info map[string]interface{}) redis.Reply {
	m := protocol.MakeMapReply()

	var flags [][]byte
	if enabled, ok := info["enabled"].(bool); ok && enabled {
		flags = append(flags, []byte("on"))
	} else {
		flags = append(flags, []byte("off"))
	}
	if mode, ok := info["mode"].(string); ok && mode != "" {
		flags = append(flags, []byte(mode))
	}
	if noloop, ok := info["noloop"].(bool); ok && noloop {
		flags = append(flags, []byte("noloop"))
	}
	m.Put("flags", protocol.MakeMultiBulkReply(flags))

	redirectVal := int64(0)
	if redirect, ok := info["redirect"].(string); ok && redirect != "" {
		if n, err := strconv.ParseInt(redirect, 10, 64); err == nil {
			redirectVal = n
		}
	}
	m.Put("redirect", protocol.MakeIntReply(redirectVal))

	var prefixes [][]byte
	if p, ok := info["prefixes"].([]string); ok {
		for _, prefix := range p {
			prefixes = append(prefixes, []byte(prefix))
		}
	}
	m.Put("prefixes", protocol.MakeMultiBulkReply(prefixes))
	return m
}

// execClientHelp returns help information
func execClientHelp(args [][]byte) redis.Reply {
	help := []string{
		"CLIENT <subcommand> [<arg> [value] [opt] ...]",
		"Subcommands:",
		"CACHING YES|NO",
		"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT mode.",
		"GETNAME",
		"    Return the name of the current connection.",
		"GETREDIR",
		"    Return the client ID we are redirecting to for tracking.",
		"ID",
		"    Return the id of the current connection.",
		"INFO",
		"    Return information about the current client connection.",
		"KILL [ip:port] [ID client-id] ...",
		"    Kill connections to clients.",
		"LIST [TYPE type] [ID id]",
		"    Return information about client connections.",
		"PAUSE timeout [WRITE|ALL]",
		"    Stop processing commands for some time.",
		"REPLY ON|OFF|SKIP",
		"    Control the replies sent to the current connection.",
		"NO-EVICT ON|OFF",
		"    Set the client eviction mode for the current connection.",
		"NO-TOUCH ON|OFF",
		"    Control whether commands may alter the LRU/LFU of keys.",
		"SETNAME connection-name",
		"    Set the current connection name.",
		"TRACKING ON|OFF [REDIRECT id] [BCAST] [...]",
		"    Enable/disable tracking for the current connection.",
		"TRACKINGINFO",
		"    Return information about server tracking of the current connection.",
		"SETINFO <attr> <value>",
		"    Set client library information (lib-name / lib-ver).",
		"UNBLOCK client-id [TIMEOUT|ERROR]",
		"    Unblock a client blocked in a blocking command from a different connection.",
		"UNPAUSE",
		"    Stop the current client pause.",
		"HELP",
		"    Print this help.",
	}

	result := make([][]byte, len(help))
	for i, line := range help {
		result[i] = []byte(line)
	}
	return protocol.MakeMultiBulkReply(result)
}

// execReadonly enables read-only mode for cluster replica
// READONLY
func execReadonly(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'readonly' command")
	}
	// Simplified: cluster not fully implemented
	return protocol.MakeOkReply()
}

// execReadwrite disables read-only mode
// READWRITE
func execReadwrite(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'readwrite' command")
	}
	// Simplified: cluster not fully implemented
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("Auth", execAuth, noPrepare, nil, -2, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
	registerCommand("Hello", execHello, noPrepare, nil, -1, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
	registerCommand("Ping", execPing, noPrepare, nil, -1, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
	registerCommand("Echo", execEcho, noPrepare, nil, 2, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagFast}, 0, 0, 0)
	registerCommand("Quit", execQuit, noPrepare, nil, 1, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagLoading, redisFlagStale, redisFlagFast}, 0, 0, 0)
	// Note: Select and SwapDB are special commands handled by Server.Exec
	registerCommand("Client", execClient, noPrepare, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerCommand("Readonly", execReadonly, noPrepare, nil, 1, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
	registerCommand("Readwrite", execReadwrite, noPrepare, nil, 1, flagFast).
		attachCommandExtra([]string{redisFlagNoScript, redisFlagFast}, 0, 0, 0)
}
