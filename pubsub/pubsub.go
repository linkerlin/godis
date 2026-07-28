package pubsub

import (
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	_subscribe    = "subscribe"
	_unsubscribe  = "unsubscribe"
	_psubscribe   = "psubscribe"
	_punsubscribe = "punsubscribe"
	_message      = "message"
	_pmessage     = "pmessage"
)

// writePush writes a Push-type reply, formatted as a RESP2 array for
// RESP2 connections or a RESP3 `>` push for connections that negotiated
// HELLO 3. kind is the push type (e.g. "subscribe", "message").
func writePush(c redis.Connection, kind string, elems ...redis.Reply) {
	if c == nil {
		return
	}
	p := protocol.MakePushReply(kind, elems)
	if c.GetProtocolVersion() == 3 {
		_, _ = c.Write(p.ToRESP3())
	} else {
		_, _ = c.Write(p.ToBytes())
	}
}

func writeUnSubNothing(c redis.Connection, psub bool) {
	kind := _unsubscribe
	if psub {
		kind = _punsubscribe
	}
	writePush(c, kind, protocol.MakeNullBulkReply(), protocol.MakeIntReply(0))
}

func makeMsg(c redis.Connection, kind string, channel string, code int64) {
	writePush(c, kind, protocol.MakeBulkReply([]byte(channel)), protocol.MakeIntReply(code))
}

/*
 * invoker should lock channel
 * return: is new subscribed
 */
func subscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.Subscribe(channel)

	raw, ok := hub.subs.Get(channel)
	var subscribers *list.LinkedList
	if ok {
		subscribers, _ = raw.(*list.LinkedList)
	} else {
		subscribers = list.Make()
		hub.subs.Put(channel, subscribers)
	}
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
}

/*
 * invoker should lock channel
 * return: is actually un-subscribe
 */
func unsubscribe0(hub *Hub, channel string, client redis.Connection) bool {
	client.UnSubscribe(channel)

	raw, ok := hub.subs.Get(channel)
	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		subscribers.RemoveAllByVal(func(a interface{}) bool {
			return utils.Equals(a, client)
		})

		if subscribers.Len() == 0 {
			hub.subs.Remove(channel)
		}
		return true
	}
	return false
}

// Subscribe puts the given connection into the given channel
func Subscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, b := range args {
		channels[i] = string(b)
	}

	hub.subsLocker.Locks(channels...)
	defer hub.subsLocker.UnLocks(channels...)

	for _, channel := range channels {
		subscribe0(hub, channel, c)
		makeMsg(c, _subscribe, channel, int64(c.SubsCount()))
	}
	return &protocol.NoReply{}
}

// UnsubscribeAll removes the given connection from all channels and patterns
func UnsubscribeAll(hub *Hub, c redis.Connection) {
	channels := c.GetChannels()

	hub.subsLocker.Locks(channels...)
	for _, channel := range channels {
		unsubscribe0(hub, channel, c)
	}
	hub.subsLocker.UnLocks(channels...)

	for _, p := range c.GetPatterns() {
		punsubscribe0(hub, p, c)
	}
}

// UnSubscribe removes the given connection from the given channel
func UnSubscribe(db *Hub, c redis.Connection, args [][]byte) redis.Reply {
	var channels []string
	if len(args) > 0 {
		channels = make([]string, len(args))
		for i, b := range args {
			channels[i] = string(b)
		}
	} else {
		channels = c.GetChannels()
	}

	db.subsLocker.Locks(channels...)
	defer db.subsLocker.UnLocks(channels...)

	if len(channels) == 0 {
		writeUnSubNothing(c, false)
		return &protocol.NoReply{}
	}

	for _, channel := range channels {
		unsubscribe0(db, channel, c)
		makeMsg(c, _unsubscribe, channel, int64(c.SubsCount()))
	}
	return &protocol.NoReply{}
}

func psubscribe0(hub *Hub, pattern string, client redis.Connection) bool {
	client.PSubscribe(pattern)
	hub.psubsMu.Lock()
	defer hub.psubsMu.Unlock()
	subscribers, ok := hub.psubs[pattern]
	if !ok {
		subscribers = list.Make()
		hub.psubs[pattern] = subscribers
	}
	if subscribers.Contains(func(a interface{}) bool {
		return a == client
	}) {
		return false
	}
	subscribers.Add(client)
	return true
}

func punsubscribe0(hub *Hub, pattern string, client redis.Connection) bool {
	client.PUnSubscribe(pattern)
	hub.psubsMu.Lock()
	defer hub.psubsMu.Unlock()
	subscribers, ok := hub.psubs[pattern]
	if !ok {
		return false
	}
	subscribers.RemoveAllByVal(func(a interface{}) bool {
		return utils.Equals(a, client)
	})
	if subscribers.Len() == 0 {
		delete(hub.psubs, pattern)
	}
	return true
}

// PSubscribe subscribes the connection to glob patterns.
func PSubscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.ArgNumErrReply{Cmd: "psubscribe"}
	}
	for _, b := range args {
		pattern := string(b)
		psubscribe0(hub, pattern, c)
		makeMsg(c, _psubscribe, pattern, int64(c.SubsCount()))
	}
	return &protocol.NoReply{}
}

// PUnSubscribe unsubscribes from patterns; empty args means all patterns.
func PUnSubscribe(hub *Hub, c redis.Connection, args [][]byte) redis.Reply {
	var patterns []string
	if len(args) > 0 {
		patterns = make([]string, len(args))
		for i, b := range args {
			patterns[i] = string(b)
		}
	} else {
		patterns = c.GetPatterns()
	}
	if len(patterns) == 0 {
		writeUnSubNothing(c, true)
		return &protocol.NoReply{}
	}
	for _, pattern := range patterns {
		punsubscribe0(hub, pattern, c)
		makeMsg(c, _punsubscribe, pattern, int64(c.SubsCount()))
	}
	return &protocol.NoReply{}
}

// Publish send msg to all subscribing client
func Publish(hub *Hub, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return &protocol.ArgNumErrReply{Cmd: "publish"}
	}
	channel := string(args[0])
	message := args[1]

	hub.subsLocker.Lock(channel)
	var channelReceivers int64
	raw, ok := hub.subs.Get(channel)
	if ok {
		subscribers, _ := raw.(*list.LinkedList)
		subscribers.ForEach(func(i int, c interface{}) bool {
			client, _ := c.(redis.Connection)
			writePush(client, _message, protocol.MakeBulkReply([]byte(channel)), protocol.MakeBulkReply(message))
			return true
		})
		channelReceivers = int64(subscribers.Len())
	}
	hub.subsLocker.UnLock(channel)

	var patternReceivers int64
	hub.psubsMu.RLock()
	for pattern, subscribers := range hub.psubs {
		match, err := wildcard.CompilePattern(pattern)
		if err != nil || !match.IsMatch(channel) {
			continue
		}
		subscribers.ForEach(func(i int, c interface{}) bool {
			client, _ := c.(redis.Connection)
			writePush(client, _pmessage, protocol.MakeBulkReply([]byte(pattern)), protocol.MakeBulkReply([]byte(channel)), protocol.MakeBulkReply(message))
			patternReceivers++
			return true
		})
	}
	hub.psubsMu.RUnlock()

	return protocol.MakeIntReply(channelReceivers + patternReceivers)
}
