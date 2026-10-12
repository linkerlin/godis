package database

// TS.READ (Redis 8.10, R5-16 final piece): cursor batch read with optional
// blocking. Wake sources: sample appends (TS.ADD/TS.MADD/TS.INCRBY/TS.DECRBY
// call signalTSWaiters) and key removal (generic removeKey hook wakes the
// waiters; the retry loop then sees the missing key and replies empty).
//
// ponytail: on timeout the current samples are flushed (partial flush, same
// as Redis); CLIENT UNBLOCK with the default/timeout mode replies an empty
// array (TS.READ never replies nil).

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

var (
	tsWaitMu  sync.Mutex
	tsWaiters = make(map[string][]*blockWaiter)
)

func registerTSWaiter(key string) *blockWaiter {
	w := &blockWaiter{
		keys:     []string{key},
		ch:       make(chan struct{}, 1),
		clientID: peekBlockingClientID(),
	}
	tsWaitMu.Lock()
	tsWaiters[key] = append(tsWaiters[key], w)
	tsWaitMu.Unlock()
	trackBlocker(w)
	return w
}

func unregisterTSWaiter(w *blockWaiter) {
	untrackBlocker(w)
	tsWaitMu.Lock()
	defer tsWaitMu.Unlock()
	for _, k := range w.keys {
		ws := tsWaiters[k]
		for i, x := range ws {
			if x == w {
				tsWaiters[k] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(tsWaiters[k]) == 0 {
			delete(tsWaiters, k)
		}
	}
}

// signalTSWaiters wakes clients blocked in TS.READ on key.
func signalTSWaiters(key string) {
	tsWaitMu.Lock()
	ws := append([]*blockWaiter(nil), tsWaiters[key]...)
	tsWaitMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

// execTSRead TS.READ key timestamp [BLOCK milliseconds min_count] [MAX_COUNT max_count]
func execTSRead(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.read' command")
	}
	key := string(args[0])

	blockMs := int64(-1) // -1 = no BLOCK
	minCount := int64(1)
	maxCount := int64(0) // 0 = unlimited
	i := 2
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "BLOCK":
			if i+2 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.read' command")
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR invalid BLOCK milliseconds")
			}
			mc, err := strconv.ParseInt(string(args[i+2]), 10, 64)
			if err != nil || mc < 1 {
				return protocol.MakeErrReply("ERR invalid BLOCK min_count")
			}
			blockMs = ms
			minCount = mc
			i += 3
		case "MAX_COUNT":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.read' command")
			}
			mc, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || mc < 1 {
				return protocol.MakeErrReply("ERR invalid MAX_COUNT")
			}
			maxCount = mc
			i += 2
		default:
			return protocol.MakeErrReply("ERR wrong number of arguments for 'ts.read' command")
		}
	}
	if blockMs >= 0 && maxCount > 0 && maxCount < minCount {
		return protocol.MakeErrReply("ERR MAX_COUNT can't be less than BLOCK min_count")
	}

	// Resolve the cursor sentinel exactly once, at command receipt.
	entity, exists := db.GetEntity(key)
	if exists {
		ts, ok := entity.Data.(*timeseries.TimeSeries)
		if !ok {
			return &protocol.WrongTypeErrReply{}
		}
		cur, errReply := resolveTSReadCursor(string(args[1]), ts)
		if errReply != nil {
			return errReply
		}
		return tsReadLoop(db, key, cur, blockMs, minCount, maxCount)
	}
	// Missing key: sentinels resolve to 0; reads stay empty (or wait).
	cur, errReply := resolveTSReadCursor(string(args[1]), nil)
	if errReply != nil {
		return errReply
	}
	return tsReadLoop(db, key, cur, blockMs, minCount, maxCount)
}

// resolveTSReadCursor maps -, +, $ (or an integer) to the inclusive cursor.
func resolveTSReadCursor(cursorStr string, ts *timeseries.TimeSeries) (int64, redis.Reply) {
	switch cursorStr {
	case "-":
		if ts == nil {
			return 0, nil
		}
		if s := ts.Range(0, math.MaxInt64); len(s) > 0 {
			return s[0].Timestamp, nil
		}
		return 0, nil
	case "+":
		if ts == nil {
			return 0, nil
		}
		if s, ok := ts.GetLast(); ok {
			return s.Timestamp, nil
		}
		return 0, nil
	case "$":
		if ts == nil {
			return 0, nil
		}
		if s, ok := ts.GetLast(); ok {
			return s.Timestamp + 1, nil
		}
		return 0, nil
	default:
		cur, err := strconv.ParseInt(cursorStr, 10, 64)
		if err != nil || cur < 0 {
			return 0, protocol.MakeErrReply("ERR invalid timestamp")
		}
		return cur, nil
	}
}

// tsReadLoop collects qualifying samples; with BLOCK it waits for appends
// until min_count is reached, the timeout flushes partially, or the key
// disappears (empty reply).
func tsReadLoop(db *DB, key string, cursor int64, blockMs, minCount, maxCount int64) redis.Reply {
	startTime := time.Now()
	for {
		reply := collectTSRead(db, key, cursor, maxCount)
		if protocol.IsErrorReply(reply) {
			return reply
		}
		arr := reply.(*protocol.MultiRawReply)
		if blockMs < 0 || int64(len(arr.Replies)) >= minCount {
			return reply
		}
		// Key removed while blocked: reply an empty array immediately.
		if _, exists := db.GetEntity(key); !exists {
			return protocol.MakeMultiRawReply(nil)
		}
		waitDur := time.Duration(blockMs) * time.Millisecond
		if blockMs > 0 {
			elapsed := time.Since(startTime)
			if elapsed >= time.Duration(blockMs)*time.Millisecond {
				return reply // timeout: flush what is available (partial)
			}
			waitDur = time.Duration(blockMs)*time.Millisecond - elapsed
		}
		w := registerTSWaiter(key)
		signaled := waitOrTimeout(w.ch, waitDur)
		unregisterTSWaiter(w)
		// CLIENT UNBLOCK: ERROR mode aborts; timeout mode flushes empty.
		if atomic.LoadInt32(&w.reason) == unblockError {
			return protocol.MakeErrReply("UNBLOCKED client unblocked via CLIENT UNBLOCK")
		}
		if !signaled {
			return collectTSRead(db, key, cursor, maxCount) // timeout flush
		}
		// signaled (append or key removal): retry
	}
}

// collectTSRead reads samples >= cursor (oldest first, MAX_COUNT capped).
// A missing key yields an empty array; a wrong-typed key an error.
func collectTSRead(db *DB, key string, cursor, maxCount int64) redis.Reply {
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeMultiRawReply(nil)
	}
	ts, ok := entity.Data.(*timeseries.TimeSeries)
	if !ok {
		return &protocol.WrongTypeErrReply{}
	}
	samples := ts.Range(cursor, math.MaxInt64)
	if maxCount > 0 && int64(len(samples)) > maxCount {
		samples = samples[:maxCount]
	}
	replies := make([]redis.Reply, 0, len(samples))
	for _, s := range samples {
		replies = append(replies, protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(s.Timestamp),
			protocol.MakeBulkReply(formatTSValue(s.Value)),
		}))
	}
	return protocol.MakeMultiRawReply(replies)
}

func init() {
	// noPrepare: the blocking loop must not hold the command-level read
	// lock while waiting (TS.ADD/DEL from other clients would deadlock
	// until the timeout); access goes through the dict's own shard locks.
	registerCommand("TS.Read", execTSRead, noPrepare, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagBlocking}, 1, 1, 1)
}
