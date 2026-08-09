package dict

import (
	"time"

	"github.com/linkerlin/godis/datastruct/lock"
	"github.com/linkerlin/godis/lib/timewheel"
)

// ExpireDict 支持字段级过期的字典
type ExpireDict struct {
	data              Dict                 // 底层字典存储数据
	expire            Dict                 // 字段 -> 过期时间的映射
	tw                *timewheel.TimeWheel // 时间轮，用于TTL管理
	mu                *lock.Locks          // 分片锁
	jobPrefix         string               // 时间轮任务 key 前缀（避免跨 hash 字段名冲突）
	onExpired         func(field string)   // 字段因 TTL 删除后回调（AOF/通知）
	allowActiveExpire func() bool          // nil = always；false 时时间轮不删（惰性仍删）
}

// NewExpireDict 创建支持字段级过期的字典
func NewExpireDict(shardCount int) *ExpireDict {
	return &ExpireDict{
		data:   MakeConcurrent(shardCount),
		expire: MakeConcurrent(shardCount),
		mu:     lock.Make(shardCount),
	}
}

// SetWithTTL 更新字段值并保留原有 TTL（用于 HINCRBY 等只改值不改 TTL 的命令）
func (ed *ExpireDict) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	ed.mu.Lock(key)
	if ed.isExpired(key) {
		cb := ed.removeExpiredLocked(key)
		ed.mu.UnLock(key)
		if cb != nil {
			cb(key)
		}
		return
	}

	ed.data.Put(key, value)
	if ttl >= 0 {
		expireTime := time.Now().Add(ttl)
		ed.expire.Put(key, expireTime)
		ed.scheduleExpireJob(key, ttl)
	}
	ed.mu.UnLock(key)
}

// SetWithExpire 设置字段并指定过期时间
func (ed *ExpireDict) SetWithExpire(key string, value interface{}, ttl time.Duration) {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)

	ed.data.Put(key, value)
	expireTime := time.Now().Add(ttl)
	ed.expire.Put(key, expireTime)
	ed.scheduleExpireJob(key, ttl)
}

// Put 设置字段（无过期），实现 dict.Dict 接口
// 如果字段原来有 TTL，会被清除（与 Redis HSET 不带 TTL 参数语义一致）
func (ed *ExpireDict) Put(key string, value interface{}) int {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)

	// 检查并删除过期标记
	if _, exists := ed.expire.Get(key); exists {
		ed.expire.Remove(key)
		ed.cancelExpireJob(key)
	}

	return ed.data.Put(key, value)
}

// Set 是 Put 的别名，保持现有代码兼容
func (ed *ExpireDict) Set(key string, value interface{}) int {
	return ed.Put(key, value)
}

// PutIfAbsent 实现 dict.Dict 接口
// 字段已过期时会被清理，随后视为不存在
func (ed *ExpireDict) PutIfAbsent(key string, value interface{}) int {
	ed.mu.Lock(key)
	var cb func(string)
	if ed.isExpired(key) {
		cb = ed.removeExpiredLocked(key)
	}
	n := ed.data.PutIfAbsent(key, value)
	ed.mu.UnLock(key)
	if cb != nil {
		cb(key)
	}
	return n
}

// PutIfExists 实现 dict.Dict 接口
// 字段不存在或已过期时返回 0；存在时覆盖值并清除 TTL
func (ed *ExpireDict) PutIfExists(key string, value interface{}) int {
	ed.mu.Lock(key)
	if ed.isExpired(key) {
		cb := ed.removeExpiredLocked(key)
		ed.mu.UnLock(key)
		if cb != nil {
			cb(key)
		}
		return 0
	}

	if _, exists := ed.data.Get(key); !exists {
		ed.mu.UnLock(key)
		return 0
	}

	ed.expire.Remove(key)
	ed.cancelExpireJob(key)
	n := ed.data.PutIfExists(key, value)
	ed.mu.UnLock(key)
	return n
}

// Get 获取字段值，自动检查过期
func (ed *ExpireDict) Get(key string) (val interface{}, exists bool) {
	ed.mu.RLock(key)
	if !ed.isExpired(key) {
		val, exists = ed.data.Get(key)
		ed.mu.RUnLock(key)
		return val, exists
	}
	ed.mu.RUnLock(key)
	ed.removeExpired(key)
	return nil, false
}

// GetWithExpire 获取字段值和剩余TTL
func (ed *ExpireDict) GetWithExpire(key string) (val interface{}, ttl time.Duration, exists bool) {
	ed.mu.RLock(key)
	if ed.isExpired(key) {
		ed.mu.RUnLock(key)
		ed.removeExpired(key)
		return nil, -2, false
	}

	val, exists = ed.data.Get(key)
	if !exists {
		ed.mu.RUnLock(key)
		return nil, -2, false
	}

	// 获取过期时间
	if expireRaw, hasExpire := ed.expire.Get(key); hasExpire {
		expireTime := expireRaw.(time.Time)
		remaining := expireTime.Sub(time.Now())
		if remaining > 0 {
			ed.mu.RUnLock(key)
			return val, remaining, true
		}
		// 刚刚过期
		ed.mu.RUnLock(key)
		ed.removeExpired(key)
		return nil, -2, false
	}

	ed.mu.RUnLock(key)
	// 没有过期时间
	return val, -1, true
}

// Delete 删除字段（现有别名）
func (ed *ExpireDict) Delete(key string) int {
	_, result := ed.Remove(key)
	return result
}

// Remove 实现 dict.Dict 接口（显式删除，不触发 onExpired）
func (ed *ExpireDict) Remove(key string) (val interface{}, result int) {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)

	ed.expire.Remove(key)
	ed.cancelExpireJob(key)
	return ed.data.Remove(key)
}

// DeleteFields 批量删除字段
func (ed *ExpireDict) DeleteFields(keys []string) int {
	ed.mu.Locks(keys...)
	defer ed.mu.UnLocks(keys...)

	deleted := 0
	for _, key := range keys {
		ed.expire.Remove(key)
		ed.cancelExpireJob(key)
		if _, result := ed.data.Remove(key); result > 0 {
			deleted++
		}
	}
	return deleted
}

// Expire 设置字段过期时间
func (ed *ExpireDict) Expire(key string, expireAt time.Time) bool {
	ed.mu.Lock(key)

	// 检查字段是否存在且未过期
	if ed.isExpired(key) {
		cb := ed.removeExpiredLocked(key)
		ed.mu.UnLock(key)
		if cb != nil {
			cb(key)
		}
		return false
	}

	if _, exists := ed.data.Get(key); !exists {
		ed.mu.UnLock(key)
		return false
	}

	ed.expire.Put(key, expireAt)
	ttl := expireAt.Sub(time.Now())
	if ttl > 0 {
		ed.scheduleExpireJob(key, ttl)
	}
	ed.mu.UnLock(key)
	return true
}

// TTL 获取字段剩余生存时间
// 返回：
//
//	-2: 字段不存在
//	-1: 字段存在但没有设置过期时间
//	>=0: 剩余秒数
func (ed *ExpireDict) TTL(key string) int64 {
	ed.mu.RLock(key)
	// 检查是否存在
	if _, exists := ed.data.Get(key); !exists {
		ed.mu.RUnLock(key)
		return -2
	}

	// 检查是否有过期时间
	expireRaw, hasExpire := ed.expire.Get(key)
	if !hasExpire {
		ed.mu.RUnLock(key)
		return -1
	}

	expireTime := expireRaw.(time.Time)
	remaining := expireTime.Sub(time.Now())

	if remaining <= 0 {
		ed.mu.RUnLock(key)
		ed.removeExpired(key)
		return -2
	}

	ed.mu.RUnLock(key)
	return int64(remaining.Seconds())
}

// PTTL 获取字段剩余生存时间（毫秒）
func (ed *ExpireDict) PTTL(key string) int64 {
	ed.mu.RLock(key)
	if _, exists := ed.data.Get(key); !exists {
		ed.mu.RUnLock(key)
		return -2
	}

	expireRaw, hasExpire := ed.expire.Get(key)
	if !hasExpire {
		ed.mu.RUnLock(key)
		return -1
	}

	expireTime := expireRaw.(time.Time)
	remaining := expireTime.Sub(time.Now())

	if remaining <= 0 {
		ed.mu.RUnLock(key)
		ed.removeExpired(key)
		return -2
	}

	ed.mu.RUnLock(key)
	return remaining.Milliseconds()
}

// Persist 移除字段的过期时间
func (ed *ExpireDict) Persist(key string) bool {
	ed.mu.Lock(key)

	if _, exists := ed.expire.Get(key); !exists {
		ed.mu.UnLock(key)
		return false
	}

	// 检查是否已过期
	if ed.isExpired(key) {
		cb := ed.removeExpiredLocked(key)
		ed.mu.UnLock(key)
		if cb != nil {
			cb(key)
		}
		return false
	}

	ed.expire.Remove(key)
	ed.cancelExpireJob(key)
	ed.mu.UnLock(key)
	return true
}

// Len 返回字段数量（不清理过期字段）
func (ed *ExpireDict) Len() int {
	return ed.data.Len()
}

// ExpireFieldCount returns how many fields currently have a per-field TTL.
func (ed *ExpireDict) ExpireFieldCount() int {
	if ed == nil || ed.expire == nil {
		return 0
	}
	return ed.expire.Len()
}

// ForEach 遍历所有字段
func (ed *ExpireDict) ForEach(consumer Consumer) {
	ed.data.ForEach(func(key string, val interface{}) bool {
		// 跳过过期字段
		if ed.isExpired(key) {
			return true
		}
		return consumer(key, val)
	})
}

// isExpired 检查字段是否过期（调用者必须持有锁）
func (ed *ExpireDict) isExpired(key string) bool {
	expireRaw, exists := ed.expire.Get(key)
	if !exists {
		return false
	}

	expireTime := expireRaw.(time.Time)
	return time.Now().After(expireTime)
}

// RandomKeys 随机获取指定数量的key
func (ed *ExpireDict) RandomKeys(limit int) []string {
	return ed.data.RandomKeys(limit)
}

// RandomDistinctKeys 随机获取不重复的key
func (ed *ExpireDict) RandomDistinctKeys(limit int) []string {
	return ed.data.RandomDistinctKeys(limit)
}

// SetTimeWheel 设置时间轮
func (ed *ExpireDict) SetTimeWheel(tw *timewheel.TimeWheel) {
	ed.tw = tw
}

// SetJobPrefix sets the time-wheel job key prefix (must be unique per hash key).
func (ed *ExpireDict) SetJobPrefix(prefix string) {
	ed.jobPrefix = prefix
}

// SetOnExpired registers a callback invoked after a field is removed due to TTL.
// Explicit deletes (HDEL / Remove) do not invoke this callback.
func (ed *ExpireDict) SetOnExpired(fn func(field string)) {
	ed.onExpired = fn
}

// SetAllowActiveExpire gates time-wheel deletions (DEBUG SET-ACTIVE-EXPIRE).
// Lazy expiry on access always deletes regardless.
func (ed *ExpireDict) SetAllowActiveExpire(fn func() bool) {
	ed.allowActiveExpire = fn
}

func (ed *ExpireDict) jobKey(field string) string {
	return ed.jobPrefix + field
}

func (ed *ExpireDict) scheduleExpireJob(field string, ttl time.Duration) {
	if ed.tw == nil || ttl < 0 {
		return
	}
	fieldCopy := field
	ed.tw.AddJob(ttl, ed.jobKey(fieldCopy), func() {
		if ed.allowActiveExpire != nil && !ed.allowActiveExpire() {
			return
		}
		ed.removeExpired(fieldCopy)
	})
}

func (ed *ExpireDict) cancelExpireJob(field string) {
	if ed.tw == nil {
		return
	}
	ed.tw.RemoveJob(ed.jobKey(field))
}

// removeExpired deletes a field if its TTL has elapsed and fires onExpired.
func (ed *ExpireDict) removeExpired(field string) {
	ed.mu.Lock(field)
	if !ed.isExpired(field) {
		// TTL may have been refreshed while waiting for the lock.
		ed.mu.UnLock(field)
		return
	}
	cb := ed.removeExpiredLocked(field)
	ed.mu.UnLock(field)
	if cb != nil {
		cb(field)
	}
}

// removeExpiredLocked removes an expired field. Caller must hold the field lock.
// Returns the onExpired callback to run after releasing the lock (may be nil).
func (ed *ExpireDict) removeExpiredLocked(field string) func(string) {
	ed.expire.Remove(field)
	ed.cancelExpireJob(field)
	_, n := ed.data.Remove(field)
	if n > 0 {
		return ed.onExpired
	}
	return nil
}

// Keys 获取所有未过期的key
func (ed *ExpireDict) Keys() []string {
	var keys []string
	ed.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// GetExpireTime returns the absolute expiration time for a field if one exists
// and the field has not expired.
func (ed *ExpireDict) GetExpireTime(key string) (time.Time, bool) {
	ed.mu.RLock(key)
	defer ed.mu.RUnLock(key)

	if ed.isExpired(key) {
		return time.Time{}, false
	}
	if raw, ok := ed.expire.Get(key); ok {
		return raw.(time.Time), true
	}
	return time.Time{}, false
}

// Clear 清空所有字段，实现 dict.Dict 接口
func (ed *ExpireDict) Clear() {
	ed.data.Clear()
	ed.expire.Clear()
}

// DictScan 实现 dict.Dict 接口，返回未过期字段的 key/value 对
func (ed *ExpireDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	// ConcurrentDict.DictScan returns keys only; attach values here for HSCAN.
	keys, nextCursor := ed.data.DictScan(cursor, count, pattern)
	if nextCursor < 0 {
		return nil, nextCursor
	}
	result := make([][]byte, 0, len(keys)*2)
	for _, kBytes := range keys {
		field := string(kBytes)
		if ed.isExpired(field) {
			continue
		}
		val, exists := ed.Get(field)
		if !exists {
			continue
		}
		bytes, ok := val.([]byte)
		if !ok {
			continue
		}
		result = append(result, kBytes, bytes)
	}
	return result, nextCursor
}
