package database

import (
	"crypto/sha1"
	"encoding/hex"
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

// countPubsubClients returns connections in pubsub state (sub/psub/ssub).
func countPubsubClients() int {
	n := 0
	RangeClients(func(c redis.Connection) bool {
		if clientConnType(c) == "pubsub" {
			n++
		}
		return true
	})
	return n
}

// countWatchingClients returns connections with active WATCH keys.
func countWatchingClients() int {
	n := 0
	RangeClients(func(c redis.Connection) bool {
		if len(c.GetWatching()) > 0 {
			n++
		}
		return true
	})
	return n
}

// countTotalWatchedKeys returns sum of WATCH keys across all connections.
func countTotalWatchedKeys() int {
	n := 0
	RangeClients(func(c redis.Connection) bool {
		n += len(c.GetWatching())
		return true
	})
	return n
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
	var flagParts []byte
	typ := clientConnType(c)
	switch typ {
	case "pubsub":
		flagParts = append(flagParts, 'P')
	case "replica", "slave":
		flagParts = append(flagParts, 'S')
	case "master":
		flagParts = append(flagParts, 'M')
	}
	if c.InMultiState() {
		flagParts = append(flagParts, 'x')
	}
	if _, blocked := activeBlockers.Load(c.GetClientID()); blocked {
		flagParts = append(flagParts, 'b')
	}
	if tid := c.GetTrackingID(); tid != "" && IsTrackingEnabled(tid) {
		flagParts = append(flagParts, 't')
	}
	if ne, ok := c.(interface{ GetNoEvict() bool }); ok && ne.GetNoEvict() {
		flagParts = append(flagParts, 'e')
	}
	if nt, ok := c.(interface{ GetNoTouch() bool }); ok && nt.GetNoTouch() {
		flagParts = append(flagParts, 'T')
	}
	if IsMonitorClient(c) {
		flagParts = append(flagParts, 'O')
	}
	if rm, ok := c.(interface{ GetReplyMode() string }); ok && rm.GetReplyMode() == "off" {
		flagParts = append(flagParts, 'n')
	}
	if len(flagParts) == 0 {
		flagParts = append(flagParts, 'N')
	}
	flags := string(flagParts)
	libName, libVer := clientLibInfo(c)
	age, idle := int64(0), int64(0)
	if t, ok := c.(interface{ AgeSeconds() int64 }); ok {
		age = t.AgeSeconds()
	}
	if t, ok := c.(interface{ IdleSeconds() int64 }); ok {
		idle = t.IdleSeconds()
	}
	subN := len(c.GetChannels())
	psubN := c.PSubsCount()
	ssubN := 0
	if sc, ok := c.(interface{ SSubsCount() int }); ok {
		ssubN = sc.SSubsCount()
	}
	user := "default"
	if u := c.GetACLUser(); u != "" {
		user = u
	}
	respVer := c.GetProtocolVersion()
	if respVer <= 0 {
		respVer = 2
	}
	cmd := "NULL"
	if lc, ok := c.(interface{ GetLastCommand() string }); ok {
		if s := lc.GetLastCommand(); s != "" {
			cmd = s
		}
	}
	multi := -1
	if c.InMultiState() {
		multi = len(c.GetQueuedCmdLine())
	}
	watching := len(c.GetWatching())
	redir := clientListRedir(c)
	argvMem, multiMem, totMem := clientListMemEstimates(c, cmd)
	caps := clientCapabilities(c)
	return fmt.Sprintf(
		"id=%d addr=%s laddr=%s fd=0 name=%s age=%d idle=%d flags=%s db=%d sub=%d psub=%d ssub=%d user=%s multi=%d watch=%d qbuf=0 qbuf-free=16384 argv-mem=%d multi-mem=%d obl=0 oll=0 omem=0 tot-mem=%d tot-cmds=%d tot-net-in=%d tot-net-out=%d rbs=0 rbp=0 events=r cmd=%s resp=%d redir=%d peerid=%s lib-name=%s lib-ver=%s capabilities=%s",
		c.GetClientID(), clientAddr(c), clientLocalAddr(c), c.GetClientName(), age, idle, flags, c.GetDBIndex(), subN, psubN, ssubN, user, multi, watching, argvMem, multiMem, totMem, 0, 0, 0, cmd, respVer, redir, clientPeerID(c), libName, libVer, caps,
	)
}

// clientPeerID returns a stable 40-hex peer id for CLIENT LIST (Redis 7.2+ peerid=).
func clientPeerID(c redis.Connection) string {
	sum := sha1.Sum([]byte(fmt.Sprintf("%d:%s", c.GetClientID(), clientAddr(c))))
	return hex.EncodeToString(sum[:])
}

func clientCapabilities(c redis.Connection) string {
	respVer := c.GetProtocolVersion()
	if respVer <= 0 {
		respVer = 2
	}
	if respVer >= 3 {
		return "resp3"
	}
	return ""
}

// clientListMemEstimates returns coarse argv-mem / multi-mem / tot-mem (not Redis-precise).
func clientListMemEstimates(c redis.Connection, cmd string) (argvMem, multiMem, totMem int64) {
	if cmd != "" && cmd != "NULL" {
		argvMem = int64(len(cmd))
	}
	if c.InMultiState() {
		for _, line := range c.GetQueuedCmdLine() {
			for _, a := range line {
				multiMem += int64(len(a))
			}
		}
	}
	totMem = argvMem + multiMem + int64(len(c.GetClientName()))
	return
}

// clientListRedir mirrors CLIENT GETREDIR: -1 off, 0 on/no redirect, else target id.
func clientListRedir(c redis.Connection) int64 {
	id := c.GetTrackingID()
	if id == "" || !IsTrackingEnabled(id) {
		return -1
	}
	info := GetTrackingInfo(id)
	redirect, _ := info["redirect"].(string)
	if redirect == "" || redirect == "0" {
		return 0
	}
	n, err := strconv.ParseInt(redirect, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// clientConnType maps a connection to Redis CLIENT LIST TYPE labels.
func clientConnType(c redis.Connection) string {
	ssub := 0
	if sc, ok := c.(interface{ SSubsCount() int }); ok {
		ssub = sc.SSubsCount()
	}
	if c.SubsCount() > 0 || c.PSubsCount() > 0 || ssub > 0 {
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
	userFilter := ""
	idFilter := map[int64]struct{}{}
	for i := 0; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "TYPE":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			typeFilter = strings.ToLower(string(args[i+1]))
			switch typeFilter {
			case "normal", "master", "replica", "slave", "pubsub":
				if typeFilter == "slave" {
					typeFilter = "replica"
				}
			default:
				return protocol.MakeErrReply("ERR Unknown client type '" + string(args[i+1]) + "'")
			}
			i += 2
		case "USER":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			userFilter = string(args[i+1])
			i += 2
		case "ID":
			i++
			if i >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			for i < len(args) {
				next := strings.ToUpper(string(args[i]))
				if next == "TYPE" || next == "ID" || next == "USER" {
					break
				}
				id, err := strconv.ParseInt(string(args[i]), 10, 64)
				if err != nil {
					return protocol.MakeErrReply("ERR Invalid client ID")
				}
				idFilter[id] = struct{}{}
				i++
			}
			if len(idFilter) == 0 {
				return protocol.MakeErrReply("ERR syntax error")
			}
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	var b strings.Builder
	RangeClients(func(other redis.Connection) bool {
		if typeFilter != "" && clientConnType(other) != typeFilter {
			return true
		}
		if userFilter != "" {
			u := other.GetACLUser()
			if u == "" {
				u = "default"
			}
			if u != userFilter {
				return true
			}
		}
		if len(idFilter) > 0 {
			if _, ok := idFilter[other.GetClientID()]; !ok {
				return true
			}
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
				if id <= 0 {
					return protocol.MakeErrReply("ERR client-id should be greater than 0")
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
