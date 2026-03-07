package dict

import (
	"time"

	"github.com/hdt3213/godis/datastruct/lock"
	"github.com/hdt3213/godis/lib/timewheel"
)

// ExpireDict 支持字段级过期的字典
type ExpireDict struct {
	data   Dict           // 底层字典存储数据
	expire Dict           // 字段 -> 过期时间的映射
	tw     *timewheel.TimeWheel // 时间轮，用于TTL管理
	mu     *lock.Locks    // 分片锁
}

// NewExpireDict 创建支持字段级过期的字典
func NewExpireDict(shardCount int) *ExpireDict {
	return &ExpireDict{
		data:   MakeConcurrent(shardCount),
		expire: MakeConcurrent(shardCount),
		mu:     lock.Make(shardCount),
	}
}

// SetWithExpire 设置字段并指定过期时间
func (ed *ExpireDict) SetWithExpire(key string, value interface{}, ttl time.Duration) {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)
	
	ed.data.Put(key, value)
	expireTime := time.Now().Add(ttl)
	ed.expire.Put(key, expireTime)
	
	// 添加到时间轮（如果配置了）
	if ed.tw != nil {
		ed.tw.AddJob(ttl, key, func() {
			ed.Delete(key)
		})
	}
}

// Set 设置字段（无过期）
func (ed *ExpireDict) Set(key string, value interface{}) int {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)
	
	// 检查并删除过期标记
	if _, exists := ed.expire.Get(key); exists {
		ed.expire.Remove(key)
	}
	
	return ed.data.Put(key, value)
}

// Get 获取字段值，自动检查过期
func (ed *ExpireDict) Get(key string) (val interface{}, exists bool) {
	ed.mu.RLock(key)
	defer ed.mu.RUnLock(key)
	
	// 先检查是否过期
	if ed.isExpired(key) {
		// 过期了，删除
		go ed.Delete(key) // 异步删除避免阻塞
		return nil, false
	}
	
	return ed.data.Get(key)
}

// GetWithExpire 获取字段值和剩余TTL
func (ed *ExpireDict) GetWithExpire(key string) (val interface{}, ttl time.Duration, exists bool) {
	ed.mu.RLock(key)
	defer ed.mu.RUnLock(key)
	
	if ed.isExpired(key) {
		go ed.Delete(key)
		return nil, -2, false
	}
	
	val, exists = ed.data.Get(key)
	if !exists {
		return nil, -2, false
	}
	
	// 获取过期时间
	if expireRaw, hasExpire := ed.expire.Get(key); hasExpire {
		expireTime := expireRaw.(time.Time)
		remaining := expireTime.Sub(time.Now())
		if remaining > 0 {
			return val, remaining, true
		}
		// 刚刚过期
		go ed.Delete(key)
		return nil, -2, false
	}
	
	// 没有过期时间
	return val, -1, true
}

// Delete 删除字段
func (ed *ExpireDict) Delete(key string) int {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)
	
	ed.expire.Remove(key)
	_, result := ed.data.Remove(key)
	return result
}

// DeleteFields 批量删除字段
func (ed *ExpireDict) DeleteFields(keys []string) int {
	ed.mu.Locks(keys...)
	defer ed.mu.UnLocks(keys...)
	
	deleted := 0
	for _, key := range keys {
		ed.expire.Remove(key)
		if _, result := ed.data.Remove(key); result > 0 {
			deleted++
		}
	}
	return deleted
}

// Expire 设置字段过期时间
func (ed *ExpireDict) Expire(key string, expireAt time.Time) bool {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)
	
	// 检查字段是否存在且未过期
	if ed.isExpired(key) {
		ed.data.Remove(key)
		return false
	}
	
	if _, exists := ed.data.Get(key); !exists {
		return false
	}
	
	ed.expire.Put(key, expireAt)
	
	// 添加到时间轮
	if ed.tw != nil {
		ttl := expireAt.Sub(time.Now())
		if ttl > 0 {
			ed.tw.AddJob(ttl, key, func() {
				ed.Delete(key)
			})
		}
	}
	
	return true
}

// TTL 获取字段剩余生存时间
// 返回：
//   -2: 字段不存在
//   -1: 字段存在但没有设置过期时间
//   >=0: 剩余秒数
func (ed *ExpireDict) TTL(key string) int64 {
	ed.mu.RLock(key)
	defer ed.mu.RUnLock(key)
	
	// 检查是否存在
	if _, exists := ed.data.Get(key); !exists {
		return -2
	}
	
	// 检查是否有过期时间
	expireRaw, hasExpire := ed.expire.Get(key)
	if !hasExpire {
		return -1
	}
	
	expireTime := expireRaw.(time.Time)
	remaining := expireTime.Sub(time.Now())
	
	if remaining <= 0 {
		// 已过期
		go ed.Delete(key)
		return -2
	}
	
	return int64(remaining.Seconds())
}

// PTTL 获取字段剩余生存时间（毫秒）
func (ed *ExpireDict) PTTL(key string) int64 {
	ed.mu.RLock(key)
	defer ed.mu.RUnLock(key)
	
	if _, exists := ed.data.Get(key); !exists {
		return -2
	}
	
	expireRaw, hasExpire := ed.expire.Get(key)
	if !hasExpire {
		return -1
	}
	
	expireTime := expireRaw.(time.Time)
	remaining := expireTime.Sub(time.Now())
	
	if remaining <= 0 {
		go ed.Delete(key)
		return -2
	}
	
	return remaining.Milliseconds()
}

// Persist 移除字段的过期时间
func (ed *ExpireDict) Persist(key string) bool {
	ed.mu.Lock(key)
	defer ed.mu.UnLock(key)
	
	if _, exists := ed.expire.Get(key); !exists {
		return false
	}
	
	// 检查是否已过期
	if ed.isExpired(key) {
		ed.data.Remove(key)
		ed.expire.Remove(key)
		return false
	}
	
	ed.expire.Remove(key)
	return true
}

// Len 返回字段数量（不清理过期字段）
func (ed *ExpireDict) Len() int {
	return ed.data.Len()
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

// Keys 获取所有未过期的key
func (ed *ExpireDict) Keys() []string {
	var keys []string
	ed.ForEach(func(key string, val interface{}) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}
