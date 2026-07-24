package database

import (
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	connpkg "github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/pubsub"
)

// execReset resets the connection state (Redis RESET).
// Does not clear authentication.
func execReset(c redis.Connection, db *DB) redis.Reply {
	if c == nil {
		return protocol.MakeStatusReply("RESET")
	}
	if c.InMultiState() {
		c.SetMultiState(false)
		c.ClearQueuedCmds()
	}
	UnWatch(c)
	c.SelectDB(0)
	c.SetClientName("")
	c.SetTrackingID("")
	c.SetProtocolVersion(2)
	if setter, ok := c.(interface {
		SetLibName(string)
		SetLibVer(string)
	}); ok {
		setter.SetLibName("")
		setter.SetLibVer("")
	}
	if db != nil && db.server != nil && db.server.hub != nil {
		pubsub.UnsubscribeAll(db.server.hub, c)
	} else {
		for _, ch := range c.GetChannels() {
			c.UnSubscribe(ch)
		}
	}
	return protocol.MakeStatusReply("RESET")
}

func clientLibInfo(c redis.Connection) (name, ver string) {
	switch v := c.(type) {
	case *connpkg.Connection:
		return v.GetLibName(), v.GetLibVer()
	case *connpkg.FakeConn:
		return v.GetLibName(), v.GetLibVer()
	default:
		if g, ok := c.(interface{ GetLibName() string }); ok {
			name = g.GetLibName()
		}
		if g, ok := c.(interface{ GetLibVer() string }); ok {
			ver = g.GetLibVer()
		}
		return name, ver
	}
}

func execClientInfoConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|info' command")
	}
	if c == nil {
		return execClientInfo(args)
	}
	return protocol.MakeBulkReply([]byte(formatClientListLine(c) + "\n"))
}

// CLIENT SETINFO LIB-NAME / LIB-VER
func execClientSetInfoConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'client|setinfo' command")
	}
	attr := strings.ToUpper(string(args[0]))
	val := string(args[1])
	setter, ok := c.(interface {
		SetLibName(string)
		SetLibVer(string)
	})
	if !ok {
		return protocol.MakeOkReply()
	}
	switch attr {
	case "LIB-NAME":
		setter.SetLibName(val)
	case "LIB-VER":
		setter.SetLibVer(val)
	default:
		return protocol.MakeErrReply("ERR Unrecognized CLIENT SETINFO attribute '" + string(args[0]) + "'")
	}
	return protocol.MakeOkReply()
}

func formatACLWhoami(c redis.Connection) redis.Reply {
	user := "default"
	if c != nil {
		if u := c.GetACLUser(); u != "" {
			user = u
		}
	}
	return protocol.MakeBulkReply([]byte(user))
}