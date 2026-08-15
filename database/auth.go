package database

import (
	"net"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/acl"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// aclExemptCommands may run before authentication or without ACL permission checks.
var aclExemptCommands = map[string]bool{
	"auth":  true,
	"hello": true,
	"ping":  true,
}

func connACLUsername(c redis.Connection) string {
	if c == nil || c.GetACLUser() == "" {
		return "default"
	}
	return c.GetACLUser()
}

func lookupACLUser(c redis.Connection) (*acl.User, bool) {
	if aclEngine == nil {
		return nil, false
	}
	return aclEngine.GetUser(connACLUsername(c))
}

// isAuthenticated reports whether the connection may execute protected commands.
func isAuthenticated(c redis.Connection) bool {
	if aclEngine != nil {
		user, ok := lookupACLUser(c)
		if !ok || !user.Enabled {
			return false
		}
		// Runtime requirepass always gates access for connections.
		if config.Properties.RequirePass != "" {
			if c == nil {
				return false
			}
			return c.GetPassword() == config.Properties.RequirePass
		}
		if !user.HasPassword() {
			return true
		}
		if c != nil && c.IsACLAuthenticated() {
			return true
		}
		return false
	}
	if config.Properties.RequirePass == "" {
		return true
	}
	if c == nil {
		return false
	}
	return c.GetPassword() == config.Properties.RequirePass
}

// checkProtectedMode rejects non-local clients when Redis-style protected mode applies:
// protected-mode yes, no requirepass / default ACL password, and bind is not loopback-only.
func checkProtectedMode(c redis.Connection) redis.Reply {
	if config.Properties == nil || !config.Properties.ProtectedMode {
		return nil
	}
	if config.Properties.RequirePass != "" {
		return nil
	}
	if aclEngine != nil {
		if u, ok := aclEngine.GetUser("default"); ok && u.HasPassword() {
			return nil
		}
	}
	if bindsOnlyLoopback(config.Properties.Bind) {
		return nil
	}
	if c == nil || isLocalClientAddr(c.RemoteAddr()) {
		return nil
	}
	return protocol.MakeErrReply(
		"DENIED Redis is running in protected mode because protected mode is enabled, " +
			"no password is set for the default user, and no connection or interface restriction " +
			"has been configured. In this mode connections are only accepted from the loopback interface. " +
			"If you want to connect from external computers to Redis you may adopt one of the following solutions: " +
			"1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' from the loopback " +
			"interface by connecting to Redis from the same host the server is running, however make sure Redis " +
			"is not publicly accessible from internet if you do so. Use CONFIG REWRITE to make this change permanent. " +
			"2) Alternatively you can just disable the protected mode by editing the Redis configuration file, " +
			"and setting the protected mode option to 'no', and then restarting the server. " +
			"3) If you started the server manually just for testing, restart it with the '--protected-mode no' option. " +
			"4) Setup a password to secure Redis.",
	)
}

func bindsOnlyLoopback(bind string) bool {
	bind = strings.TrimSpace(bind)
	if bind == "" {
		// Empty bind historically means all interfaces in Redis.
		return false
	}
	parts := strings.FieldsFunc(bind, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t'
	})
	if len(parts) == 0 {
		return false
	}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if p == "0.0.0.0" || p == "*" || p == "::" || p == "[::]" {
			return false
		}
		host := p
		if strings.HasPrefix(host, "[") {
			if i := strings.Index(host, "]"); i > 0 {
				host = host[1:i]
			}
		} else if strings.Count(host, ":") == 1 {
			// host:port form — rare in bind directive
			h, _, err := net.SplitHostPort(host)
			if err == nil {
				host = h
			}
		}
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return false
		}
	}
	return true
}

func isLocalClientAddr(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		// Tests / unix-like anonymous: treat as local.
		return true
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	} else {
		host = strings.Trim(addr, "[]")
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// checkACLPermission verifies the connection's ACL user may run cmdName
// (and its keys / pubsub channels when applicable).
// CheckACLPermission is the exported entry point for ACL command enforcement,
// used by the cluster command path (which maintains its own dispatch loop).
func CheckACLPermission(c redis.Connection, cmdName string, args [][]byte) redis.Reply {
	return checkACLPermission(c, cmdName, args)
}

func checkACLPermission(c redis.Connection, cmdName string, args [][]byte) redis.Reply {
	if aclEngine == nil || aclExemptCommands[cmdName] {
		return nil
	}
	user, ok := lookupACLUser(c)
	if !ok || !user.Enabled {
		return protocol.MakeErrReply("NOPERM ACL user disabled or missing")
	}
	if !user.CheckCommand(cmdName) {
		addACLLogEntry(c, "command", "toplevel", cmdName, user.Name)
		return protocol.MakeErrReply("NOPERM User " + user.Name + " has no permissions to run the '" + strings.ToUpper(cmdName) + "' command")
	}

	var writeKeys, readKeys []string
	if cmd, ok := cmdTable[cmdName]; ok && cmd.prepare != nil && len(args) > 0 {
		writeKeys, readKeys = cmd.prepare(args)
	}
	if !user.CheckPermission(cmdName, writeKeys, readKeys) {
		addACLLogEntry(c, "key", "toplevel", cmdName, user.Name)
		return protocol.MakeErrReply("NOPERM this user has no permissions to access one of the keys used as arguments")
	}

	switch cmdName {
	case "publish", "spublish":
		if len(args) >= 1 && !user.CheckChannel(string(args[0])) {
			addACLLogEntry(c, "channel", "toplevel", string(args[0]), user.Name)
			return protocol.MakeErrReply("NOPERM this user has no permissions to access one of the channels used as arguments")
		}
	case "subscribe", "ssubscribe", "psubscribe":
		for _, a := range args {
			ch := string(a)
			if !user.CheckChannel(ch) {
				addACLLogEntry(c, "channel", "toplevel", ch, user.Name)
				return protocol.MakeErrReply("NOPERM this user has no permissions to access one of the channels used as arguments")
			}
		}
	}
	return nil
}

// authenticateCredentials validates username/password without binding a connection.
func authenticateCredentials(username, password string) redis.Reply {
	if aclEngine != nil {
		_, err := aclEngine.Authenticate(username, password)
		if err != nil {
			return protocol.MakeErrReply("ERR invalid username or password")
		}
		// Honor runtime requirepass for the default user (tests / CONFIG SET).
		if config.Properties.RequirePass != "" && username == "default" && password != config.Properties.RequirePass {
			return protocol.MakeErrReply("ERR invalid password")
		}
		return protocol.MakeOkReply()
	}
	if config.Properties.RequirePass == "" {
		return protocol.MakeErrReply("ERR Client sent AUTH, but no password is set")
	}
	if password != config.Properties.RequirePass {
		return protocol.MakeErrReply("ERR invalid password")
	}
	return protocol.MakeOkReply()
}

func bindAuthToConnection(c redis.Connection, username, password string) {
	if c == nil {
		return
	}
	c.SetACLUser(username)
	c.SetACLAuthenticated(true)
	c.SetPassword(password)
}

// Auth validates credentials and updates connection authentication state.
// AUTH [username] password
func Auth(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	username := "default"
	password := string(args[0])
	if len(args) == 2 {
		username = string(args[0])
		password = string(args[1])
	}
	reply := authenticateCredentials(username, password)
	if _, ok := reply.(*protocol.OkReply); !ok {
		if errReply, isErr := reply.(*protocol.StandardErrReply); isErr {
			return errReply
		}
		return reply
	}
	bindAuthToConnection(c, username, password)
	return protocol.MakeOkReply()
}

// execAuth is the DB.Exec path for AUTH (e.g. redis.call("AUTH")).
func execAuth(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 || len(args) > 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'auth' command")
	}
	username := "default"
	password := string(args[0])
	if len(args) == 2 {
		username = string(args[0])
		password = string(args[1])
	}
	return authenticateCredentials(username, password)
}

// Hello negotiates RESP version and optionally authenticates the connection.
func Hello(c redis.Connection, args [][]byte) redis.Reply {
	return HelloWithRole(c, args, "master")
}

// HelloWithRole is HELLO with an explicit replication role ("master"/"slave").
func HelloWithRole(c redis.Connection, args [][]byte, role string) redis.Reply {
	protoVersion := 2
	var username, password, clientName string
	if role == "" {
		role = "master"
	}

	i := 0
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "SETNAME":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR Syntax error in HELLO option 'SETNAME'")
			}
			clientName = string(args[i+1])
			i += 2
		case "AUTH":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR Syntax error in HELLO option 'AUTH'")
			}
			username = string(args[i+1])
			password = string(args[i+2])
			i += 3
		default:
			if v, err := strconv.Atoi(string(args[i])); err == nil {
				if v != 2 && v != 3 {
					return protocol.MakeErrReply("NOPROTO unsupported protocol version")
				}
				protoVersion = v
				i++
			} else {
				return protocol.MakeErrReply("ERR Protocol version is not an integer or out of range")
			}
		}
	}

	if username != "" && password != "" {
		reply := authenticateCredentials(username, password)
		if _, ok := reply.(*protocol.OkReply); !ok {
			if errReply, isErr := reply.(*protocol.StandardErrReply); isErr {
				if strings.HasPrefix(errReply.Status, "ERR invalid") {
					return protocol.MakeErrReply("WRONGPASS invalid username-password pair or user is disabled.")
				}
			}
			return reply
		}
		bindAuthToConnection(c, username, password)
	}

	clientID := "0"
	if c != nil {
		c.SetProtocolVersion(protoVersion)
		if clientName != "" {
			c.SetClientName(clientName)
		}
		clientID = strconv.FormatInt(c.GetClientID(), 10)
	}

	if protoVersion == 3 {
		idNum, _ := strconv.ParseInt(clientID, 10, 64)
		m := protocol.MakeMapReply()
		m.Put("server", protocol.MakeBulkReply([]byte("godis")))
		m.Put("version", protocol.MakeBulkReply([]byte("8.0.0")))
		m.Put("proto", protocol.MakeIntReply(int64(protoVersion)))
		m.Put("id", protocol.MakeIntReply(idNum))
		m.Put("mode", protocol.MakeBulkReply([]byte(getRedisMode())))
		m.Put("role", protocol.MakeBulkReply([]byte(role)))
		m.Put("modules", protocol.MakeEmptyMultiBulkReply())
		return m
	}

	var result [][]byte
	result = append(result, []byte("server"), []byte("godis"))
	result = append(result, []byte("version"), []byte("8.0.0"))
	result = append(result, []byte("proto"), []byte(strconv.Itoa(protoVersion)))
	result = append(result, []byte("id"), []byte(clientID))
	result = append(result, []byte("mode"), []byte(getRedisMode()))
	result = append(result, []byte("role"), []byte(role))
	result = append(result, []byte("modules"))
	result = append(result, protocol.MakeEmptyMultiBulkReply().ToBytes())
	return protocol.MakeMultiBulkReply(result)
}

// execHello is the DB.Exec path when connection context is unavailable.
func execHello(db *DB, args [][]byte) redis.Reply {
	return Hello(nil, args)
}
