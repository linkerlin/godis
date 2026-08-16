package database

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

type SlowLogEntry struct {
	ID         int64
	Timestamp  time.Time
	Duration   int64
	Command    [][]byte
	PeerId     string
	ClientName string
}

// SlowLogger Slow query logger
type SlowLogger struct {
	mu         sync.RWMutex
	entries    []*SlowLogEntry
	count      int
	nextIdx    int
	maxEntries int

	threshold   int64
	nextID      int64
	logCommands [][]byte
}

func NewSlowLogger(maxEntries int, threshold int64) *SlowLogger {
	entries := make([]*SlowLogEntry, maxEntries)
	return &SlowLogger{
		entries:     entries,
		maxEntries:  maxEntries,
		threshold:   threshold,
		nextID:      1,
		logCommands: [][]byte{},
	}
}

func (sl *SlowLogger) Record(start time.Time, args [][]byte, peerAddr, clientName string) {
	if sl == nil || len(sl.entries) == 0 {
		return
	}
	duration := time.Since(start)
	micros := duration.Microseconds()

	if micros < sl.threshold {
		return
	}

	sl.mu.Lock()
	defer sl.mu.Unlock()

	entry := &SlowLogEntry{
		ID:         sl.nextID,
		Timestamp:  start,
		Duration:   micros,
		Command:    args,
		PeerId:     peerAddr,
		ClientName: clientName,
	}

	sl.nextID++

	// 插入到环形数组
	sl.entries[sl.nextIdx] = entry
	sl.nextIdx = (sl.nextIdx + 1) % sl.maxEntries

	// 更新条目计数
	if sl.count < sl.maxEntries {
		sl.count++
	}
}

func (sl *SlowLogger) SetThreshold(threshold int64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.threshold = threshold
}

// SetMaxLen resizes the slowlog ring buffer and trims older entries if needed.
func (sl *SlowLogger) SetMaxLen(newMax int) {
	if newMax < 0 {
		return
	}
	sl.mu.Lock()
	defer sl.mu.Unlock()

	kept := sl.collectEntriesLocked(sl.count)
	sl.maxEntries = newMax
	sl.count = 0
	sl.nextIdx = 0
	if newMax == 0 {
		sl.entries = nil
		return
	}

	sl.entries = make([]*SlowLogEntry, newMax)
	if len(kept) > newMax {
		kept = kept[:newMax]
	}
	for i := len(kept) - 1; i >= 0; i-- {
		sl.entries[sl.nextIdx] = kept[i]
		sl.nextIdx = (sl.nextIdx + 1) % sl.maxEntries
		sl.count++
	}
}

func (sl *SlowLogger) collectEntriesLocked(count int) []*SlowLogEntry {
	if count <= 0 || sl.maxEntries == 0 || len(sl.entries) == 0 {
		return nil
	}
	if count > sl.count {
		count = sl.count
	}
	result := make([]*SlowLogEntry, 0, count)
	startIdx := (sl.nextIdx - 1 + sl.maxEntries) % sl.maxEntries
	for i := 0; i < count; i++ {
		idx := (startIdx - i + sl.maxEntries) % sl.maxEntries
		result = append(result, sl.entries[idx])
	}
	return result
}

func (sl *SlowLogger) GetEntries(count int) []*SlowLogEntry {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	if count <= 0 || sl.count == 0 {
		return []*SlowLogEntry{}
	}

	if count > sl.count {
		count = sl.count
	}

	return sl.collectEntriesLocked(count)
}

func (sl *SlowLogger) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.count
}

func (sl *SlowLogger) Reset() {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// 重置环形数组状态
	sl.entries = make([]*SlowLogEntry, sl.maxEntries)
	sl.count = 0
	sl.nextIdx = 0
	sl.nextID = 1
}

// HandleSlowlogCommand Process SLOWLOG command
func (sl *SlowLogger) HandleSlowlogCommand(args [][]byte) redis.Reply {
	argsLen := len(args)
	if argsLen <= 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'SLOWLOG' command")
	}

	subCmd := strings.ToLower(string(args[1]))

	switch subCmd {
	case "get":
		if argsLen > 3 {
			return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'GET'. Try SLOWLOG HELP.")
		}
		count := 10
		if argsLen == 3 {
			n, err := strconv.Atoi(string(args[2]))
			// Redis: count must be >= -1; -1 means return all entries.
			if err != nil || n < -1 {
				return protocol.MakeErrReply("ERR count should be greater than or equal to -1")
			}
			if n == -1 {
				n = sl.Len()
			}
			count = n
		}
		entries := sl.GetEntries(count)
		return formatSlowlogEntries(entries)
	case "len":
		if argsLen != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'slowlog|len' command")
		}
		return protocol.MakeIntReply(int64(sl.Len()))
	case "reset":
		if argsLen != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'slowlog|reset' command")
		}
		sl.Reset()
		return protocol.MakeOkReply()
	case "help":
		if argsLen != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'slowlog|help' command")
		}
		return protocol.MakeMultiBulkReply([][]byte{
			[]byte("SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
			[]byte("GET [<count>]"),
			[]byte("    Return top <count> entries from the slowlog (default: 10, -1 mean all)."),
			[]byte("    Entries are made of:"),
			[]byte("    id, timestamp, time in microseconds, arguments array, client IP and port,"),
			[]byte("    client name"),
			[]byte("LEN"),
			[]byte("    Return the length of the slowlog."),
			[]byte("RESET"),
			[]byte("    Reset the slowlog."),
			[]byte("HELP"),
			[]byte("    Print this help."),
		})
	default:
		return protocol.MakeErrReply("ERR unknown subcommand '" + subCmd + "'. Try SLOWLOG HELP.")
	}
}

func formatSlowlogEntries(entries []*SlowLogEntry) redis.Reply {
	result := make([]redis.Reply, 0, len(entries))
	for _, log := range entries {
		logList := make([]redis.Reply, 0, 6)
		logList = append(logList, protocol.MakeIntReply(log.ID),
			protocol.MakeIntReply(log.Timestamp.Unix()),
			protocol.MakeIntReply(int64(log.Duration)),
			protocol.MakeMultiBulkReply(log.Command),
			protocol.MakeBulkReply([]byte(log.PeerId)),
			protocol.MakeBulkReply([]byte(log.ClientName)))
		result = append(result, protocol.MakeMultiRawReply(logList))
	}
	return protocol.MakeMultiRawReply(result)
}
