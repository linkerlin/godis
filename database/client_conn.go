package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execClientConn handles CLIENT subcommands with connection context.
func execClientConn(c redis.Connection, db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client' command")
	}

	subCmd := strings.ToUpper(string(args[0]))
	switch subCmd {
	case "LIST":
		return execClientListConn(c, args[1:])
	case "INFO":
		return execClientInfoConn(c, args[1:])
	case "SETNAME":
		return execClientSetNameConn(c, args[1:])
	case "GETNAME":
		return execClientGetNameConn(c, args[1:])
	case "SETINFO":
		return execClientSetInfoConn(c, args[1:])
	case "KILL":
		return execClientKillConn(c, args[1:])
	case "PAUSE":
		return execClientPause(db, args[1:])
	case "UNPAUSE":
		return execClientUnpause(db, args[1:])
	case "ID":
		return execClientIDConn(c, args[1:])
	case "REPLY":
		return execClientReply(args[1:])
	case "TRACKING":
		return execClientTrackingConn(c, args[1:])
	case "CACHING":
		return execClientCaching(args[1:])
	case "GETREDIR":
		return execClientGetRedir(args[1:])
	case "TRACKINGINFO":
		return execClientTrackingInfoConn(c, args[1:])
	case "NO-EVICT":
		return execClientNoEvictConn(c, args[1:])
	case "UNBLOCK":
		return execClientUnblock(args[1:])
	case "HELP":
		return execClientHelp(args[1:])
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + strings.ToLower(subCmd) + "'")
	}
}

func execClientSetNameConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|setname' command")
	}
	name := string(args[0])
	if strings.Contains(name, " ") {
		return protocol.MakeErrReply("ERR Client names cannot contain spaces, newlines or special characters.")
	}
	c.SetClientName(name)
	return protocol.MakeOkReply()
}

func execClientGetNameConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|getname' command")
	}
	if name := c.GetClientName(); name != "" {
		return protocol.MakeBulkReply([]byte(name))
	}
	return protocol.MakeNullBulkReply()
}

func execClientIDConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|id' command")
	}
	return protocol.MakeIntReply(c.GetClientID())
}

func execClientTrackingConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|tracking' command")
	}

	// CLIENT TRACKING requires RESP3 because invalidations are sent as Push messages.
	if c.GetProtocolVersion() != 3 {
		return protocol.MakeErrReply("ERR CLIENT TRACKING is only supported in RESP3 mode")
	}

	mode := strings.ToUpper(string(args[0]))
	if mode == "OFF" {
		if id := c.GetTrackingID(); id != "" {
			DisableTracking(id)
			c.SetTrackingID("")
		}
		return protocol.MakeStatusReply("OK")
	}
	if mode != "ON" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	trackMode := ""
	var prefixes []string
	redirectID := ""
	noLoop := false
	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "REDIRECT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			if _, err := strconv.Atoi(string(args[i+1])); err != nil {
				return protocol.MakeErrReply("ERR Invalid client ID")
			}
			redirectID = string(args[i+1])
			i++
		case "PREFIX":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			prefixes = append(prefixes, string(args[i+1]))
			i++
		case "BCAST":
			trackMode = "bcast"
		case "OPTIN":
			trackMode = "optin"
		case "OPTOUT":
			trackMode = "optout"
		case "NOLOOP":
			noLoop = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if prev := c.GetTrackingID(); prev != "" {
		DisableTracking(prev)
	}
	id := EnableTracking(c, trackMode, prefixes, redirectID, noLoop)
	c.SetTrackingID(id)
	return protocol.MakeStatusReply("OK")
}

func execClientTrackingInfoConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|trackinginfo' command")
	}

	var info map[string]interface{}
	if id := c.GetTrackingID(); id != "" {
		info = GetTrackingInfo(id)
	} else {
		info = map[string]interface{}{
			"enabled":  false,
			"mode":     "",
			"prefixes": []string{},
		}
	}

	var result [][]byte
	result = append(result, []byte("flags"))
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
	result = append(result, protocol.MakeMultiBulkReply(flags).ToBytes())

	result = append(result, []byte("redirect"))
	if redirect, ok := info["redirect"].(string); ok && redirect != "" {
		result = append(result, []byte(redirect))
	} else {
		result = append(result, []byte("0"))
	}

	result = append(result, []byte("prefixes"))
	var prefixReply [][]byte
	if p, ok := info["prefixes"].([]string); ok {
		for _, prefix := range p {
			prefixReply = append(prefixReply, []byte(prefix))
		}
	}
	result = append(result, protocol.MakeMultiBulkReply(prefixReply).ToBytes())

	return protocol.MakeMultiBulkReply(result)
}

func applyCacheHooks(c redis.Connection, cmdName string, write, read []string, failed bool) {
	if failed {
		return
	}
	if len(write) > 0 && !isReadOnlyCommand(cmdName) {
		InvalidateKeysOnWrite(write)
	}
	if c == nil || len(read) == 0 || !isReadOnlyCommand(cmdName) {
		return
	}
	if id := c.GetTrackingID(); id != "" {
		TrackKeysOnRead(id, read)
	}
}
