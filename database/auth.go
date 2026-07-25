package database

import (
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

// checkACLPermission verifies the connection's ACL user may run cmdName.
func checkACLPermission(c redis.Connection, cmdName string) redis.Reply {
	if aclEngine == nil || aclExemptCommands[cmdName] {
		return nil
	}
	user, ok := lookupACLUser(c)
	if !ok || !user.Enabled {
		return protocol.MakeErrReply("NOPERM ACL user disabled or missing")
	}
	if !user.CheckCommand(cmdName) {
		addACLLogEntry("command", "toplevel", cmdName, user.Name)
		return protocol.MakeErrReply("NOPERM User " + user.Name + " has no permissions to run the '" + strings.ToUpper(cmdName) + "' command")
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
		case "AUTH":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			username = string(args[i+1])
			password = string(args[i+2])
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			clientName = string(args[i+1])
			i += 2
		default:
			if v, err := strconv.Atoi(string(args[i])); err == nil {
				if v != 2 && v != 3 {
					return protocol.MakeErrReply("ERR Protocol version not supported")
				}
				protoVersion = v
				i++
			} else {
				return protocol.MakeSyntaxErrReply()
			}
		}
	}

	if username != "" && password != "" {
		reply := authenticateCredentials(username, password)
		if _, ok := reply.(*protocol.OkReply); !ok {
			if errReply, isErr := reply.(*protocol.StandardErrReply); isErr {
				if strings.HasPrefix(errReply.Status, "ERR invalid") {
					return protocol.MakeErrReply("WRONGPASS invalid username-password pair")
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
		m := protocol.MakeMapReply()
		m.Put("server", protocol.MakeBulkReply([]byte("godis")))
		m.Put("version", protocol.MakeBulkReply([]byte("8.0.0")))
		m.Put("proto", protocol.MakeBulkReply([]byte(strconv.Itoa(protoVersion))))
		m.Put("id", protocol.MakeBulkReply([]byte(clientID)))
		m.Put("mode", protocol.MakeBulkReply([]byte("standalone")))
		m.Put("role", protocol.MakeBulkReply([]byte(role)))
		m.Put("modules", protocol.MakeEmptyMultiBulkReply())
		return m
	}

	var result [][]byte
	result = append(result, []byte("server"), []byte("godis"))
	result = append(result, []byte("version"), []byte("8.0.0"))
	result = append(result, []byte("proto"), []byte(strconv.Itoa(protoVersion)))
	result = append(result, []byte("id"), []byte(clientID))
	result = append(result, []byte("mode"), []byte("standalone"))
	result = append(result, []byte("role"), []byte(role))
	result = append(result, []byte("modules"))
	result = append(result, protocol.MakeEmptyMultiBulkReply().ToBytes())
	return protocol.MakeMultiBulkReply(result)
}

// execHello is the DB.Exec path when connection context is unavailable.
func execHello(db *DB, args [][]byte) redis.Reply {
	return Hello(nil, args)
}
