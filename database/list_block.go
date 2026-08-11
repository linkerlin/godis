package database

import (
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// parseBlockTimeout parses BLPOP/BRPOP/BLMPOP/BZPOP*/BZMPOP/BRPOPLPUSH timeout
// (seconds, float). Aligns Redis: negative vs NaN/parse vs Inf/overflow.
func parseBlockTimeout(arg []byte) (time.Duration, redis.Reply) {
	timeoutSec, err := strconv.ParseFloat(string(arg), 64)
	if err != nil || math.IsNaN(timeoutSec) {
		return 0, protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	if timeoutSec < 0 {
		return 0, protocol.MakeErrReply("ERR timeout is negative")
	}
	if math.IsInf(timeoutSec, 0) {
		return 0, protocol.MakeErrReply("ERR timeout is out of range")
	}
	d := time.Duration(timeoutSec * float64(time.Second))
	if timeoutSec > 0 && d <= 0 {
		return 0, protocol.MakeErrReply("ERR timeout is out of range")
	}
	return d, nil
}

// Blocking waiters: signal + retry. Commands manage their own key locks (noPrepare).
var (
	listWaitMu  sync.Mutex
	listWaiters = make(map[string][]*blockWaiter) // key -> waiters
	zsetWaitMu  sync.Mutex
	zsetWaiters = make(map[string][]*blockWaiter)

	// goroutine id -> client id (set around blocking command execution)
	boundBlockingClient sync.Map
	// client id -> active waiter (for CLIENT UNBLOCK)
	activeBlockers sync.Map
)

const (
	unblockNone    int32 = 0
	unblockTimeout int32 = 1
	unblockError   int32 = 2
)

type blockWaiter struct {
	keys     []string
	ch       chan struct{}
	clientID int64
	reason   int32
}

func goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	s := string(buf[:n])
	s = strings.TrimPrefix(s, "goroutine ")
	i := strings.IndexByte(s, ' ')
	if i <= 0 {
		return 0
	}
	id, _ := strconv.ParseInt(s[:i], 10, 64)
	return id
}

// BindBlockingClientID associates the current goroutine with a client for waiter registration.
func BindBlockingClientID(id int64) {
	if id == 0 {
		return
	}
	boundBlockingClient.Store(goid(), id)
}

// ClearBlockingClientID clears the goroutine binding.
func ClearBlockingClientID() {
	boundBlockingClient.Delete(goid())
}

var boundNoTouch sync.Map // goid -> struct{}

func bindNoTouch() {
	boundNoTouch.Store(goid(), struct{}{})
}

func clearNoTouch() {
	boundNoTouch.Delete(goid())
}

func peekNoTouch() bool {
	_, ok := boundNoTouch.Load(goid())
	return ok
}

func peekBlockingClientID() int64 {
	if v, ok := boundBlockingClient.Load(goid()); ok {
		return v.(int64)
	}
	return 0
}

// UnblockClientByID wakes a client blocked on BLPOP/BZPOP/… (CLIENT UNBLOCK).
// mode: ""|"TIMEOUT" → null reply; "ERROR" → UNBLOCKED error.
func UnblockClientByID(clientID int64, mode string) bool {
	v, ok := activeBlockers.Load(clientID)
	if !ok {
		return false
	}
	w := v.(*blockWaiter)
	if strings.EqualFold(mode, "ERROR") {
		atomic.StoreInt32(&w.reason, unblockError)
	} else {
		atomic.StoreInt32(&w.reason, unblockTimeout)
	}
	select {
	case w.ch <- struct{}{}:
	default:
	}
	return true
}

func trackBlocker(w *blockWaiter) {
	if w.clientID != 0 {
		activeBlockers.Store(w.clientID, w)
	}
}

func untrackBlocker(w *blockWaiter) {
	if w.clientID != 0 {
		activeBlockers.Delete(w.clientID)
	}
}

func replyAfterWait(w *blockWaiter, signaled bool) redis.Reply {
	reason := atomic.LoadInt32(&w.reason)
	if reason == unblockError {
		return protocol.MakeErrReply("UNBLOCKED client unblocked via CLIENT UNBLOCK")
	}
	if !signaled || reason == unblockTimeout {
		return protocol.MakeNullBulkReply()
	}
	return nil // retry
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

// GetBlockedZSetClientsCount counts clients blocked on BZPOP*/BZMPOP wait queues.
func GetBlockedZSetClientsCount() int64 {
	zsetWaitMu.Lock()
	defer zsetWaitMu.Unlock()
	seen := make(map[*blockWaiter]struct{})
	for _, ws := range zsetWaiters {
		for _, w := range ws {
			seen[w] = struct{}{}
		}
	}
	return int64(len(seen))
}

func registerListWaiter(keys []string) *blockWaiter {
	w := &blockWaiter{
		keys:     append([]string(nil), keys...),
		ch:       make(chan struct{}, 1),
		clientID: peekBlockingClientID(),
	}
	listWaitMu.Lock()
	for _, k := range keys {
		listWaiters[k] = append(listWaiters[k], w)
	}
	listWaitMu.Unlock()
	trackBlocker(w)
	return w
}

func unregisterListWaiter(w *blockWaiter) {
	untrackBlocker(w)
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
	w := &blockWaiter{
		keys:     append([]string(nil), keys...),
		ch:       make(chan struct{}, 1),
		clientID: peekBlockingClientID(),
	}
	zsetWaitMu.Lock()
	for _, k := range keys {
		zsetWaiters[k] = append(zsetWaiters[k], w)
	}
	zsetWaitMu.Unlock()
	trackBlocker(w)
	return w
}

func unregisterZSetWaiter(w *blockWaiter) {
	untrackBlocker(w)
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
	timeout, errReply := parseBlockTimeout(args[len(args)-1])
	if errReply != nil {
		return errReply
	}
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
		if r := replyAfterWait(w, signaled); r != nil {
			return r
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
	timeout, errReply := parseBlockTimeout(args[4])
	if errReply != nil {
		return errReply
	}
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
		if r := replyAfterWait(w, signaled); r != nil {
			return r
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
	if numKeys <= 0 {
		return protocol.MakeErrReply("ERR numkeys should be greater than 0")
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
		if count <= 0 {
			return protocol.MakeErrReply("ERR count should be greater than 0")
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
	// Minimum without COUNT: timeout numkeys key LEFT|RIGHT → 4 args.
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blmpop' command")
	}
	timeout, errReply := parseBlockTimeout(args[0])
	if errReply != nil {
		return errReply
	}

	numKeys, err := strconv.Atoi(string(args[1]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if numKeys <= 0 {
		return protocol.MakeErrReply("ERR numkeys should be greater than 0")
	}
	// Need numkeys keys + LEFT|RIGHT after timeout/numkeys.
	if len(args) < 3+numKeys {
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
		if r := replyAfterWait(w, signaled); r != nil {
			return r
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
