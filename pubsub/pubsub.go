package pubsub

import (
	"strconv"

	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	_subscribe         = "subscribe"
	_unsubscribe       = "unsubscribe"
	_psubscribe        = "psubscribe"
	_punsubscribe      = "punsubscribe"
	messageBytes       = []byte("message")
	pmessageBytes      = []byte("pmessage")
	unSubscribeNothing = []byte("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n")
	pUnSubNothing      = []byte("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n")
)

func makeMsg(t string, channel string, code int64) []byte {
	return []byte("*3\r\n$" + strconv.FormatInt(int64(len(t)), 10) + protocol.CRLF + t + protocol.CRLF +
		"$" + strconv.FormatInt(int64(len(channel)), 10) + protocol.CRLF + channel + protocol.CRLF +
		":" + strconv.FormatInt(code, 10) + protocol.CRLF)
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
		_, _ = c.Write(makeMsg(_subscribe, channel, int64(c.SubsCount())))
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
		_, _ = c.Write(unSubscribeNothing)
		return &protocol.NoReply{}
	}

	for _, channel := range channels {
		unsubscribe0(db, channel, c)
		_, _ = c.Write(makeMsg(_unsubscribe, channel, int64(c.SubsCount())))
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
		_, _ = c.Write(makeMsg(_psubscribe, pattern, int64(c.SubsCount())))
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
		_, _ = c.Write(pUnSubNothing)
		return &protocol.NoReply{}
	}
	for _, pattern := range patterns {
		punsubscribe0(hub, pattern, c)
		_, _ = c.Write(makeMsg(_punsubscribe, pattern, int64(c.SubsCount())))
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
			replyArgs := make([][]byte, 3)
			replyArgs[0] = messageBytes
			replyArgs[1] = []byte(channel)
			replyArgs[2] = message
			_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
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
			replyArgs := [][]byte{pmessageBytes, []byte(pattern), []byte(channel), message}
			_, _ = client.Write(protocol.MakeMultiBulkReply(replyArgs).ToBytes())
			patternReceivers++
			return true
		})
	}
	hub.psubsMu.RUnlock()

	return protocol.MakeIntReply(channelReceivers + patternReceivers)
}
