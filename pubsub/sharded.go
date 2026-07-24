package pubsub

import (
	"strconv"
	"sync"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// ShardedHub manages sharded pub/sub channels
type ShardedHub struct {
	// slot -> channel -> subscribers
	slots map[int]map[string]map[redis.Connection]struct{}
	mu    sync.RWMutex
}

// NewShardedHub creates a new sharded pub/sub hub
func NewShardedHub() *ShardedHub {
	return &ShardedHub{
		slots: make(map[int]map[string]map[redis.Connection]struct{}),
	}
}

func (sh *ShardedHub) getSlot(channel string) int {
	hash := 0
	for i := 0; i < len(channel); i++ {
		hash = ((hash << 5) - hash) + int(channel[i])
		hash = hash & 0x7FFF
	}
	return hash % 16384
}

func (sh *ShardedHub) subCount(conn redis.Connection) int {
	n := 0
	for _, slotMap := range sh.slots {
		for _, subs := range slotMap {
			if _, ok := subs[conn]; ok {
				n++
			}
		}
	}
	return n
}

// Subscribe subscribes a connection to sharded channels and writes confirmations.
func (sh *ShardedHub) Subscribe(conn redis.Connection, channels []string) redis.Reply {
	if conn == nil {
		return protocol.MakeErrReply("ERR client closed connection")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for _, channel := range channels {
		slot := sh.getSlot(channel)
		if sh.slots[slot] == nil {
			sh.slots[slot] = make(map[string]map[redis.Connection]struct{})
		}
		if sh.slots[slot][channel] == nil {
			sh.slots[slot][channel] = make(map[redis.Connection]struct{})
		}
		sh.slots[slot][channel][conn] = struct{}{}
		conn.Subscribe(channel)
		count := sh.subCount(conn)
		msg := []byte("*3\r\n$10\r\nssubscribe\r\n$" +
			strconv.Itoa(len(channel)) + "\r\n" + channel + "\r\n:" +
			strconv.Itoa(count) + "\r\n")
		_, _ = conn.Write(msg)
	}
	return &protocol.NoReply{}
}

// Unsubscribe unsubscribes from sharded channels.
func (sh *ShardedHub) Unsubscribe(conn redis.Connection, channels []string) redis.Reply {
	if conn == nil {
		return protocol.MakeErrReply("ERR client closed connection")
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()

	if len(channels) == 0 {
		var all []string
		for _, slotMap := range sh.slots {
			for channel, subs := range slotMap {
				if _, ok := subs[conn]; ok {
					all = append(all, channel)
				}
			}
		}
		channels = all
		if len(channels) == 0 {
			_, _ = conn.Write([]byte("*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n"))
			return &protocol.NoReply{}
		}
	}

	for _, channel := range channels {
		slot := sh.getSlot(channel)
		if slotMap, ok := sh.slots[slot]; ok {
			if subs, ok := slotMap[channel]; ok {
				delete(subs, conn)
				if len(subs) == 0 {
					delete(slotMap, channel)
				}
			}
			if len(slotMap) == 0 {
				delete(sh.slots, slot)
			}
		}
		conn.UnSubscribe(channel)
		count := sh.subCount(conn)
		msg := []byte("*3\r\n$12\r\nsunsubscribe\r\n$" +
			strconv.Itoa(len(channel)) + "\r\n" + channel + "\r\n:" +
			strconv.Itoa(count) + "\r\n")
		_, _ = conn.Write(msg)
	}
	return &protocol.NoReply{}
}

// Publish publishes a message to a sharded channel
func (sh *ShardedHub) Publish(channel string, message []byte) int {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	slot := sh.getSlot(channel)
	slotMap, ok := sh.slots[slot]
	if !ok {
		return 0
	}
	subs, ok := slotMap[channel]
	if !ok {
		return 0
	}

	n := 0
	reply := MakeSMessageReply(channel, message)
	for conn := range subs {
		if conn == nil {
			continue
		}
		_, _ = conn.Write(reply.ToBytes())
		n++
	}
	return n
}

// Channels returns all subscribed channel names.
func (sh *ShardedHub) Channels() []string {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	seen := make(map[string]struct{})
	var out []string
	for _, slotMap := range sh.slots {
		for ch, subs := range slotMap {
			if len(subs) == 0 {
				continue
			}
			if _, ok := seen[ch]; ok {
				continue
			}
			seen[ch] = struct{}{}
			out = append(out, ch)
		}
	}
	return out
}

// GetSlot returns the slot for a channel
func (sh *ShardedHub) GetSlot(channel string) int {
	return sh.getSlot(channel)
}

// AfterClientClose cleans up when client disconnects
func (sh *ShardedHub) AfterClientClose(conn redis.Connection) {
	if conn == nil {
		return
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for slot, slotMap := range sh.slots {
		for channel, subs := range slotMap {
			if _, ok := subs[conn]; ok {
				delete(subs, conn)
				conn.UnSubscribe(channel)
				if len(subs) == 0 {
					delete(slotMap, channel)
				}
			}
		}
		if len(slotMap) == 0 {
			delete(sh.slots, slot)
		}
	}
}

// MakeSMessageReply creates a sharded message reply
func MakeSMessageReply(channel string, message []byte) *protocol.MultiBulkReply {
	return protocol.MakeMultiBulkReply([][]byte{[]byte("smessage"), []byte(channel), message})
}
