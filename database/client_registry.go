package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// clientRegistry tracks live connections for CLIENT LIST / KILL.
var clientRegistry sync.Map // redis.Connection -> struct{}

// RegisterClient adds a connection to the global client table.
func RegisterClient(c redis.Connection) {
	if c != nil {
		clientRegistry.Store(c, struct{}{})
	}
}

// UnregisterClient removes a connection from the global client table.
func UnregisterClient(c redis.Connection) {
	if c != nil {
		clientRegistry.Delete(c)
	}
}

// RangeClients invokes fn for each registered connection.
func RangeClients(fn func(redis.Connection) bool) {
	clientRegistry.Range(func(key, _ interface{}) bool {
		c, ok := key.(redis.Connection)
		if !ok || c == nil {
			return true
		}
		return fn(c)
	})
}

// FindClientByID returns a registered connection with the given client id, or nil.
func FindClientByID(id int64) redis.Connection {
	var found redis.Connection
	RangeClients(func(c redis.Connection) bool {
		if c.GetClientID() == id {
			found = c
			return false
		}
		return true
	})
	return found
}

func clientAddr(c redis.Connection) string {
	addr := c.RemoteAddr()
	if addr == "" {
		return "127.0.0.1:0"
	}
	return addr
}

func clientLocalAddr(c redis.Connection) string {
	if t, ok := c.(interface{ LocalAddr() string }); ok {
		if a := t.LocalAddr(); a != "" {
			return a
		}
	}
	return "127.0.0.1:6399"
}

func clientAgeSeconds(c redis.Connection) int64 {
	if t, ok := c.(interface{ AgeSeconds() int64 }); ok {
		return t.AgeSeconds()
	}
	return 0
}

func formatClientListLine(c redis.Connection) string {
	flags := "N"
	typ := clientConnType(c)
	switch typ {
	case "pubsub":
		flags = "P"
	case "replica", "slave":
		flags = "S"
	case "master":
		flags = "M"
	}
	if c.InMultiState() {
		flags = "x"
	}
	libName, libVer := clientLibInfo(c)
	age, idle := int64(0), int64(0)
	if t, ok := c.(interface{ AgeSeconds() int64 }); ok {
		age = t.AgeSeconds()
	}
	if t, ok := c.(interface{ IdleSeconds() int64 }); ok {
		idle = t.IdleSeconds()
	}
	return fmt.Sprintf(
		"id=%d addr=%s laddr=%s fd=0 name=%s age=%d idle=%d flags=%s db=%d sub=%d psub=%d multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client lib-name=%s lib-ver=%s",
		c.GetClientID(), clientAddr(c), clientLocalAddr(c), c.GetClientName(), age, idle, flags, c.GetDBIndex(), c.SubsCount(), c.PSubsCount(), libName, libVer,
	)
}

// clientConnType maps a connection to Redis CLIENT LIST TYPE labels.
func clientConnType(c redis.Connection) string {
	if c.SubsCount() > 0 || c.PSubsCount() > 0 {
		return "pubsub"
	}
	if c.IsSlave() {
		return "replica"
	}
	if c.IsMaster() {
		return "master"
	}
	return "normal"
}

// execClientListConn lists registered clients (CLIENT LIST).
func execClientListConn(c redis.Connection, args [][]byte) redis.Reply {
	typeFilter := ""
	if len(args) == 2 && strings.EqualFold(string(args[0]), "TYPE") {
		typeFilter = strings.ToLower(string(args[1]))
		switch typeFilter {
		case "normal", "master", "replica", "slave", "pubsub":
			if typeFilter == "slave" {
				typeFilter = "replica"
			}
		default:
			return protocol.MakeErrReply("ERR Unknown client type '" + string(args[1]) + "'")
		}
	} else if len(args) != 0 {
		return protocol.MakeErrReply("ERR syntax error")
	}

	var b strings.Builder
	RangeClients(func(other redis.Connection) bool {
		if typeFilter != "" && clientConnType(other) != typeFilter {
			return true
		}
		b.WriteString(formatClientListLine(other))
		b.WriteByte('\n')
		return true
	})
	return protocol.MakeBulkReply([]byte(b.String()))
}

// execClientKillConn kills matching clients (CLIENT KILL).
func execClientKillConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|kill' command")
	}

	skipMe := true
	filterID := int64(-1)
	filterAddr := ""
	filterLAddr := ""
	filterType := ""
	filterUser := ""
	filterMaxAge := int64(-1)
	oldStyle := false

	if len(args) == 1 {
		oldStyle = true
		filterAddr = string(args[0])
	} else {
		for i := 0; i < len(args); {
			opt := strings.ToUpper(string(args[i]))
			switch opt {
			case "ID":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				id, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid client ID")
				}
				filterID = id
				i += 2
			case "ADDR":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				filterAddr = string(args[i+1])
				i += 2
			case "LADDR":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				filterLAddr = string(args[i+1])
				i += 2
			case "TYPE":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				filterType = strings.ToLower(string(args[i+1]))
				switch filterType {
				case "normal", "master", "replica", "slave", "pubsub":
					if filterType == "slave" {
						filterType = "replica"
					}
				default:
					return protocol.MakeErrReply("ERR Unknown client type '" + string(args[i+1]) + "'")
				}
				i += 2
			case "USER":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				filterUser = string(args[i+1])
				i += 2
			case "MAXAGE":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
				if err != nil || n < 0 {
					return protocol.MakeErrReply("ERR value is not an integer or out of range")
				}
				filterMaxAge = n
				i += 2
			case "SKIPME":
				if i+1 >= len(args) {
					return protocol.MakeSyntaxErrReply()
				}
				v := strings.ToUpper(string(args[i+1]))
				if v == "YES" {
					skipMe = true
				} else if v == "NO" {
					skipMe = false
				} else {
					return protocol.MakeSyntaxErrReply()
				}
				i += 2
			default:
				return protocol.MakeSyntaxErrReply()
			}
		}
	}

	killed := 0
	RangeClients(func(other redis.Connection) bool {
		if skipMe && c != nil && other == c {
			return true
		}
		if filterID >= 0 && other.GetClientID() != filterID {
			return true
		}
		if filterAddr != "" && clientAddr(other) != filterAddr {
			return true
		}
		if filterLAddr != "" && clientLocalAddr(other) != filterLAddr {
			return true
		}
		if filterType != "" && clientConnType(other) != filterType {
			return true
		}
		if filterUser != "" && other.GetACLUser() != filterUser {
			return true
		}
		if filterMaxAge >= 0 && clientAgeSeconds(other) < filterMaxAge {
			return true
		}
		_ = other.Close()
		UnregisterClient(other)
		killed++
		return true
	})

	if oldStyle {
		if killed == 0 {
			return protocol.MakeErrReply("ERR No such client")
		}
		return protocol.MakeOkReply()
	}
	return protocol.MakeIntReply(int64(killed))
}

// execClientNoEvictConn handles CLIENT NO-EVICT ON|OFF (flag only; no eviction engine yet).
func execClientNoEvictConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|no-evict' command")
	}
	mode := strings.ToUpper(string(args[0]))
	setter, ok := c.(interface{ SetNoEvict(bool) })
	if !ok {
		return protocol.MakeOkReply()
	}
	switch mode {
	case "ON":
		setter.SetNoEvict(true)
	case "OFF":
		setter.SetNoEvict(false)
	default:
		return protocol.MakeSyntaxErrReply()
	}
	return protocol.MakeOkReply()
}

// execClientNoTouchConn handles CLIENT NO-TOUCH ON|OFF.
func execClientNoTouchConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|no-touch' command")
	}
	mode := strings.ToUpper(string(args[0]))
	setter, ok := c.(interface{ SetNoTouch(bool) })
	if !ok {
		return protocol.MakeOkReply()
	}
	switch mode {
	case "ON":
		setter.SetNoTouch(true)
	case "OFF":
		setter.SetNoTouch(false)
	default:
		return protocol.MakeSyntaxErrReply()
	}
	return protocol.MakeOkReply()
}

// execClientReplyConn handles CLIENT REPLY ON|OFF|SKIP.
func execClientReplyConn(c redis.Connection, args [][]byte) redis.Reply {
	if c == nil {
		return protocol.MakeErrReply("ERR CLIENT REPLY requires a client connection")
	}
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|reply' command")
	}
	mode := strings.ToUpper(string(args[0]))
	switch mode {
	case "ON", "OFF", "SKIP":
		if setter, ok := c.(interface{ SetReplyMode(string) }); ok {
			setter.SetReplyMode(mode)
		}
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply("ERR syntax error")
	}
}
