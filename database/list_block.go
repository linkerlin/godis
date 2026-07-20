package database

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// Blocking waiters: signal + retry. Commands manage their own key locks (noPrepare).
var (
	listWaitMu   sync.Mutex
	listWaiters  = make(map[string][]*blockWaiter) // key -> waiters
	zsetWaitMu   sync.Mutex
	zsetWaiters  = make(map[string][]*blockWaiter)
)

type blockWaiter struct {
	keys []string
	ch   chan struct{}
}

// GetBlockedListClientsCount counts clients blocked on list wait queues.
func GetBlockedListClientsCount() int64 {
	listWaitMu.Lock()
	defer listWaitMu.Unlock()
	seen := make(map[*blockWaiter]struct{})
	for _, ws := range listWaiters {
		for _, w := range ws {
			seen[w] = struct{}{}
		}
	}
	return int64(len(seen))
}

func registerListWaiter(keys []string) *blockWaiter {
	w := &blockWaiter{keys: append([]string(nil), keys...), ch: make(chan struct{}, 1)}
	listWaitMu.Lock()
	for _, k := range keys {
		listWaiters[k] = append(listWaiters[k], w)
	}
	listWaitMu.Unlock()
	return w
}

func unregisterListWaiter(w *blockWaiter) {
	listWaitMu.Lock()
	defer listWaitMu.Unlock()
	for _, k := range w.keys {
		ws := listWaiters[k]
		for i, x := range ws {
			if x == w {
				listWaiters[k] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(listWaiters[k]) == 0 {
			delete(listWaiters, k)
		}
	}
}

// signalListWaiters wakes waiters blocked on key (e.g. after LPUSH/RPUSH).
func signalListWaiters(key string) {
	listWaitMu.Lock()
	ws := append([]*blockWaiter(nil), listWaiters[key]...)
	listWaitMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func registerZSetWaiter(keys []string) *blockWaiter {
	w := &blockWaiter{keys: append([]string(nil), keys...), ch: make(chan struct{}, 1)}
	zsetWaitMu.Lock()
	for _, k := range keys {
		zsetWaiters[k] = append(zsetWaiters[k], w)
	}
	zsetWaitMu.Unlock()
	return w
}

func unregisterZSetWaiter(w *blockWaiter) {
	zsetWaitMu.Lock()
	defer zsetWaitMu.Unlock()
	for _, k := range w.keys {
		ws := zsetWaiters[k]
		for i, x := range ws {
			if x == w {
				zsetWaiters[k] = append(ws[:i], ws[i+1:]...)
				break
			}
		}
		if len(zsetWaiters[k]) == 0 {
			delete(zsetWaiters, k)
		}
	}
}

func signalZSetWaiters(key string) {
	zsetWaitMu.Lock()
	ws := append([]*blockWaiter(nil), zsetWaiters[key]...)
	zsetWaitMu.Unlock()
	for _, w := range ws {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func waitOrTimeout(ch <-chan struct{}, timeout time.Duration) bool {
	if timeout == 0 {
		<-ch
		return true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return true
	case <-timer.C:
		return false
	}
}

// tryListPopLeft attempts LPOP under write lock of key. Returns (key, value, ok) or error reply.
func tryListPop(db *DB, keys []string, left bool) (string, []byte, redis.Reply) {
	db.RWLocks(keys, nil)
	defer db.RWUnLocks(keys, nil)
	for _, key := range keys {
		list, errReply := db.getAsList(key)
		if errReply != nil {
			return "", nil, errReply
		}
		if list == nil || list.Len() == 0 {
			continue
		}
		var val []byte
		if left {
			val = list.Remove(0).([]byte)
		} else {
			val = list.Remove(list.Len() - 1).([]byte)
		}
		if list.Len() == 0 {
			db.Remove(key)
		}
		db.addVersion(key)
		if left {
			db.addAof(utils.ToCmdLine3("lpop", []byte(key)))
		} else {
			db.addAof(utils.ToCmdLine3("rpop", []byte(key)))
		}
		return key, val, nil
	}
	return "", nil, nil
}

// execBLPop BLPOP key [key ...] timeout
func execBLPop(db *DB, args [][]byte) redis.Reply {
	return execBlockingListPop(db, args, true)
}

// execBRPop BRPOP key [key ...] timeout
func execBRPop(db *DB, args [][]byte) redis.Reply {
	return execBlockingListPop(db, args, false)
}

func execBlockingListPop(db *DB, args [][]byte, left bool) redis.Reply {
	cmd := "blpop"
	if !left {
		cmd = "brpop"
	}
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for '" + cmd + "' command")
	}
	timeoutSec, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil || timeoutSec < 0 {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i])
	}

	for {
		key, val, errReply := tryListPop(db, keys, left)
		if errReply != nil {
			return errReply
		}
		if key != "" {
			return protocol.MakeMultiBulkReply([][]byte{[]byte(key), val})
		}
		w := registerListWaiter(keys)
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterListWaiter(w)
		if !signaled {
			return protocol.MakeNullBulkReply()
		}
		// signaled: retry pop
	}
}

// execBLMove BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
func execBLMove(db *DB, args [][]byte) redis.Reply {
	if len(args) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blmove' command")
	}
	source := string(args[0])
	srcSide := strings.ToUpper(string(args[2]))
	dstSide := strings.ToUpper(string(args[3]))
	timeoutSec, err := strconv.ParseFloat(string(args[4]), 64)
	if err != nil || timeoutSec < 0 {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))
	if srcSide != "LEFT" && srcSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}
	if dstSide != "LEFT" && dstSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}

	lockKeys := []string{source, string(args[1])}
	for {
		db.RWLocks(lockKeys, nil)
		result := execLMove(db, args[:4])
		db.RWUnLocks(lockKeys, nil)
		if _, ok := result.(*protocol.NullBulkReply); !ok {
			return result
		}
		w := registerListWaiter([]string{source})
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterListWaiter(w)
		if !signaled {
			return protocol.MakeNullBulkReply()
		}
	}
}

// execLMove LMOVE source destination LEFT|RIGHT LEFT|RIGHT
func execLMove(db *DB, args [][]byte) redis.Reply {
	if len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lmove' command")
	}

	source := string(args[0])
	destination := string(args[1])
	srcSide := strings.ToUpper(string(args[2]))
	dstSide := strings.ToUpper(string(args[3]))

	if srcSide != "LEFT" && srcSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}
	if dstSide != "LEFT" && dstSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}

	srcList, errReply := db.getAsList(source)
	if errReply != nil {
		return errReply
	}
	if srcList == nil || srcList.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}

	var val interface{}
	if srcSide == "LEFT" {
		val = srcList.Remove(0)
	} else {
		val = srcList.Remove(srcList.Len() - 1)
	}
	if srcList.Len() == 0 {
		db.Remove(source)
	}

	dstList, _, dstErrReply := db.getOrInitList(destination)
	if dstErrReply != nil {
		return dstErrReply
	}
	if dstSide == "LEFT" {
		dstList.Insert(0, val)
	} else {
		dstList.Add(val)
	}

	db.addAof(utils.ToCmdLine3("lmove", args...))
	signalListWaiters(destination)
	return protocol.MakeBulkReply(val.([]byte))
}

// execLMPop removes and returns elements from multiple lists
// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
func execLMPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lmpop' command")
	}

	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if len(args) < 1+numKeys+1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lmpop' command")
	}

	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}

	direction := strings.ToUpper(string(args[1+numKeys]))
	if direction != "LEFT" && direction != "RIGHT" {
		return protocol.MakeErrReply("ERR syntax error")
	}

	count := 1
	idx := 2 + numKeys
	if idx < len(args) && strings.ToUpper(string(args[idx])) == "COUNT" {
		if idx+1 >= len(args) {
			return protocol.MakeErrReply("ERR syntax error")
		}
		count, err = strconv.Atoi(string(args[idx+1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
	}

	for _, key := range keys {
		list, errReply := db.getAsList(key)
		if errReply != nil {
			return errReply
		}
		if list == nil || list.Len() == 0 {
			continue
		}

		var values [][]byte
		popCount := count
		if popCount > list.Len() {
			popCount = list.Len()
		}
		for i := 0; i < popCount; i++ {
			var val interface{}
			if direction == "LEFT" {
				val = list.Remove(0)
			} else {
				val = list.Remove(list.Len() - 1)
			}
			values = append(values, val.([]byte))
		}
		if list.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLine3("lmpop", args...))
		result := []redis.Reply{
			protocol.MakeBulkReply([]byte(key)),
			protocol.MakeMultiBulkReply(values),
		}
		return protocol.MakeMultiRawReply(result)
	}
	return protocol.MakeNullBulkReply()
}

// execBLMPop is the blocking version of LMPop
// BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
func execBLMPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blmpop' command")
	}
	timeoutSec, err := strconv.ParseFloat(string(args[0]), 64)
	if err != nil || timeoutSec < 0 {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil || numKeys < 1 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if len(args) < 2+numKeys {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blmpop' command")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[2+i])
	}

	for {
		db.RWLocks(keys, nil)
		result := execLMPop(db, args[1:])
		db.RWUnLocks(keys, nil)
		if _, ok := result.(*protocol.NullBulkReply); !ok {
			return result
		}
		w := registerListWaiter(keys)
		signaled := waitOrTimeout(w.ch, timeout)
		unregisterListWaiter(w)
		if !signaled {
			return protocol.MakeNullBulkReply()
		}
	}
}

func init() {
	registerCommand("BLPop", execBLPop, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("BRPop", execBRPop, nil, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("LMove", execLMove, prepareRPopLPush, undoRPopLPush, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 2, 1)
	registerCommand("BLMove", execBLMove, nil, nil, 6, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, 2, 1)
	registerCommand("LMPop", execLMPop, prepareLMPop, nil, -4, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -2, 1)
	registerCommand("BLMPop", execBLMPop, nil, nil, -5, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
}

func prepareLMPop(args [][]byte) ([]string, []string) {
	if len(args) < 1 {
		return nil, nil
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 1 || len(args) < 1+numKeys {
		return nil, nil
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	return keys, nil
}
