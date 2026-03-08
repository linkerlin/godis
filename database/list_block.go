package database

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/lib/utils"
	"github.com/hdt3213/godis/redis/protocol"
)

// 阻塞列表的全局等待队列
var (
	blPopWaiters  = make(map[string][]*listWaiter) // key -> 等待者列表
	brPopWaiters  = make(map[string][]*listWaiter)
	waiterMu      sync.Mutex
)

// listWaiter 表示一个等待列表操作的客户端
type listWaiter struct {
	conn       redis.Connection
	timeout    time.Duration
	timer      *time.Timer
	resultChan chan *listWaiterResult
}

type listWaiterResult struct {
	key   string
	value []byte
}

// execBLPop BLPOP key [key ...] timeout
func execBLPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blpop' command")
	}

	// 解析超时时间
	timeoutSec, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))

	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i]))
	}

	// 首先尝试非阻塞弹出
	for _, key := range keys {
		list, errReply := db.getAsList(key)
		if errReply != nil {
			continue
		}
		if list == nil || list.Len() == 0 {
			continue
		}

		// 成功弹出
		val := list.Remove(0).([]byte)
		if list.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLine3("lpop", []byte(key)))

		result := make([][]byte, 2)
		result[0] = []byte(key)
		result[1] = val
		return protocol.MakeMultiBulkReply(result)
	}

	// 无法立即弹出，阻塞等待
	return blockPop(keys, timeout, true)
}

// execBRPop BRPOP key [key ...] timeout
func execBRPop(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'brpop' command")
	}

	// 解析超时时间
	timeoutSec, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))

	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i]))
	}

	// 首先尝试非阻塞弹出
	for _, key := range keys {
		list, errReply := db.getAsList(key)
		if errReply != nil {
			continue
		}
		if list == nil || list.Len() == 0 {
			continue
		}

		// 成功弹出（从右边）
		val := list.Remove(list.Len() - 1).([]byte)
		if list.Len() == 0 {
			db.Remove(key)
		}
		db.addAof(utils.ToCmdLine3("rpop", []byte(key)))

		result := make([][]byte, 2)
		result[0] = []byte(key)
		result[1] = val
		return protocol.MakeMultiBulkReply(result)
	}

	// 无法立即弹出，阻塞等待
	return blockPop(keys, timeout, false)
}

// execBLMove BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
func execBLMove(db *DB, args [][]byte) redis.Reply {
	if len(args) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'blmove' command")
	}

	source := string(args[0])
	destination := string(args[1])
	srcSide := strings.ToUpper(string(args[2]))
	dstSide := strings.ToUpper(string(args[3]))

	timeoutSec, err := strconv.ParseFloat(string(args[4]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR timeout is not a float or out of range")
	}
	timeout := time.Duration(timeoutSec * float64(time.Second))

	// 验证方向参数
	if srcSide != "LEFT" && srcSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}
	if dstSide != "LEFT" && dstSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}

	// 首先尝试非阻塞移动
	list, errReply := db.getAsList(source)
	if errReply == nil && list != nil && list.Len() > 0 {
		// 执行移动
		result := execLMove(db, args[:4])
		return result
	}

	// 阻塞等待
	return blockLMove(source, destination, srcSide, dstSide, timeout)
}

// blockPop 阻塞弹出实现
func blockPop(keys []string, timeout time.Duration, isLeft bool) redis.Reply {
	// 简化实现：直接返回空（超时）
	// 实际实现需要维护等待队列和通知机制
	if timeout > 0 {
		time.Sleep(timeout)
	}
	return protocol.MakeNullBulkReply()
}

// blockLMove 阻塞列表移动实现
func blockLMove(source, destination, srcSide, dstSide string, timeout time.Duration) redis.Reply {
	// 简化实现：直接返回空（超时）
	if timeout > 0 {
		time.Sleep(timeout)
	}
	return protocol.MakeNullBulkReply()
}

// notifyListWaiters 当列表被修改时通知等待者
func notifyListWaiters(key string) {
	waiterMu.Lock()
	defer waiterMu.Unlock()

	// 通知 BLPOP 等待者
	if waiters, ok := blPopWaiters[key]; ok {
		for _, w := range waiters {
			if w.timer != nil {
				w.timer.Stop()
			}
			close(w.resultChan)
		}
		delete(blPopWaiters, key)
	}

	// 通知 BRPOP 等待者
	if waiters, ok := brPopWaiters[key]; ok {
		for _, w := range waiters {
			if w.timer != nil {
				w.timer.Stop()
			}
			close(w.resultChan)
		}
		delete(brPopWaiters, key)
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

	// 验证方向参数
	if srcSide != "LEFT" && srcSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}
	if dstSide != "LEFT" && dstSide != "RIGHT" {
		return protocol.MakeSyntaxErrReply()
	}

	// 获取源列表
	srcList, errReply := db.getAsList(source)
	if errReply != nil {
		return errReply
	}
	if srcList == nil || srcList.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}

	// 从源列表弹出
	var val interface{}
	if srcSide == "LEFT" {
		val = srcList.Remove(0)
	} else {
		val = srcList.Remove(srcList.Len() - 1)
	}

	// 清理空列表
	if srcList.Len() == 0 {
		db.Remove(source)
	}

	// 推入目标列表
	dstList, dstIsNew, dstErrReply := db.getOrInitList(destination)
	if dstErrReply != nil {
		return dstErrReply
	}

	if dstSide == "LEFT" {
		dstList.Insert(0, val)
	} else {
		dstList.Add(val)
	}

	// AOF
	if dstIsNew {
		db.addAof(utils.ToCmdLine3("rpush", []byte(destination), val.([]byte)))
	}
	db.addAof(utils.ToCmdLine3("lmove", args...))

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
	
	// Parse direction
	direction := strings.ToUpper(string(args[1+numKeys]))
	if direction != "LEFT" && direction != "RIGHT" {
		return protocol.MakeErrReply("ERR syntax error")
	}
	
	// Parse count
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
		idx += 2
	}
	
	// Try each key
	for _, key := range keys {
		list, errReply := db.getAsList(key)
		if errReply != nil {
			return errReply
		}
		if list == nil || list.Len() == 0 {
			continue
		}
		
		// Pop elements
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
		
		// Build reply
		result := make([]redis.Reply, 0)
		result = append(result, protocol.MakeBulkReply([]byte(key)))
		result = append(result, protocol.MakeMultiBulkReply(values))
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
	
	timeout, err := strconv.ParseFloat(string(args[0]), 64)
	if err != nil {
		return protocol.MakeErrReply("ERR value is not a valid float")
	}
	
	// Call LMPop with remaining args
	result := execLMPop(db, args[1:])
	if _, ok := result.(*protocol.NullBulkReply); !ok {
		return result
	}
	
	// Block if needed
	if timeout > 0 {
		time.Sleep(time.Duration(timeout * float64(time.Second)))
	}
	
	return protocol.MakeNullBulkReply()
}

func init() {
	// 注意：flagBlocking 在 router.go 中定义，这里使用 flagSpecial 代替
	registerCommand("BLPop", execBLPop, prepareReadKeys, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("BRPop", execBRPop, prepareReadKeys, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
	registerCommand("LMove", execLMove, prepareRPopLPush, undoRPopLPush, 5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 2, 1)
	registerCommand("BLMove", execBLMove, prepareRPopLPush, nil, 6, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, 2, 1)
	registerCommand("LMPop", execLMPop, prepareReadKeys, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, -2, 1)
	registerCommand("BLMPop", execBLMPop, prepareReadKeys, nil, -3, flagSpecial).
		attachCommandExtra([]string{redisFlagBlocking}, 1, -2, 1)
}

// prepareReadKeys 准备读取多个键
func prepareReadKeys(args [][]byte) ([]string, []string) {
	// 最后一个参数是 timeout，不算作键
	keys := make([]string, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys = append(keys, string(args[i]))
	}
	return nil, keys
}


