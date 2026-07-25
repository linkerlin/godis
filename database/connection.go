package database

import (
	"fmt"
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

// execClientList lists client connections
// CLIENT LIST [TYPE normal|master|replica|pubsub]
func execClientList(args [][]byte) redis.Reply {
	// Simplified: return empty list
	// In full implementation, would iterate over all connections
	return protocol.MakeBulkReply([]byte(""))
}

// execClientInfo returns info about current client
// CLIENT INFO
func execClientInfo(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|info' command")
	}

	// Build client info string
	info := fmt.Sprintf("id=1 addr=127.0.0.1:12345 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\n")
	return protocol.MakeBulkReply([]byte(info))
}

// execClientSetName sets client name
// CLIENT SETNAME connection-name
func execClientSetName(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|setname' command")
	}

	name := string(args[0])
	if strings.Contains(name, " ") {
		return protocol.MakeErrReply("ERR Client names cannot contain spaces, newlines or special characters.")
	}

	// Simplified: would store in connection
	_ = name
	return protocol.MakeOkReply()
}

// execClientGetName gets client name
// CLIENT GETNAME
func execClientGetName(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|getname' command")
	}

	// Simplified: return nil (no name set)
	return &protocol.NullBulkReply{}
}

// execClientKill kills client connections
// CLIENT KILL [ip:port] [ID client-id] [TYPE type] [USER username] [ADDR ip:port] [SKIPME yes/no]
func execClientKill(args [][]byte) redis.Reply {
	// Simplified: always return 0 (no clients killed)
	// In full implementation, would parse filters and kill matching clients
	return protocol.MakeIntReply(0)
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

// execClientID returns current client ID
// CLIENT ID
func execClientID(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|id' command")
	}

	// Simplified: return 1
	return protocol.MakeIntReply(1)
}

// execClientReply controls command replies (legacy path without conn — no-op store).
// CLIENT REPLY ON|OFF|SKIP
func execClientReply(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|reply' command")
	}
	mode := strings.ToUpper(string(args[0]))
	switch mode {
	case "ON", "OFF", "SKIP":
		return protocol.MakeStatusReply("OK")
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

// execClientTrackingInfo returns tracking information
// CLIENT TRACKINGINFO
func execClientTrackingInfo(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|trackinginfo' command")
	}

	// Get current connection info
	// Simplified: return default tracking info
	info := map[string]interface{}{
		"enabled":  false,
		"mode":     "",
		"prefixes": []string{},
	}

	var result [][]byte

	// Tracking status
	result = append(result, []byte("flags"))
	var flags [][]byte
	if info["enabled"].(bool) {
		flags = append(flags, []byte("on"))
	} else {
		flags = append(flags, []byte("off"))
	}
	mode := info["mode"].(string)
	if mode == "bcast" {
		flags = append(flags, []byte("bcast"))
	}
	result = append(result, protocol.MakeMultiBulkReply(flags).ToBytes())

	// Redirect target (0 if not redirected)
	result = append(result, []byte("redirect"))
	result = append(result, []byte("0"))

	// Prefixes
	result = append(result, []byte("prefixes"))
	var prefixes [][]byte
	if p, ok := info["prefixes"].([]string); ok {
		for _, prefix := range p {
			prefixes = append(prefixes, []byte(prefix))
		}
	}
	result = append(result, protocol.MakeMultiBulkReply(prefixes).ToBytes())

	return protocol.MakeMultiBulkReply(result)
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
		"UNBLOCK client-id [TIMEOUT|ERROR]",
		"    Unblock a client blocked in a blocking command from a different connection.",
		"UNPAUSE",
		"    Stop the current client pause.",
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
