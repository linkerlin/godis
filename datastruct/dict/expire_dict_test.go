package dict

import (
	"testing"
	"time"
)

func TestExpireDict_SetWithExpire(t *testing.T) {
	ed := NewExpireDict(16)
	
	// 设置带过期时间的字段
	ed.SetWithExpire("key1", "value1", time.Second)
	
	// 立即获取应该存在
	val, exists := ed.Get("key1")
	if !exists {
		t.Error("Get() should return exists=true immediately after SetWithExpire")
	}
	if val != "value1" {
		t.Errorf("Get() = %v, want value1", val)
	}
	
	// 检查TTL
	ttl := ed.TTL("key1")
	if ttl < 0 || ttl > 1 {
		t.Errorf("TTL() = %d, want between 0 and 1", ttl)
	}
}

func TestExpireDict_Expire(t *testing.T) {
	ed := NewExpireDict(16)
	
	// 先设置普通字段
	ed.Set("key1", "value1")
	
	// 设置过期时间
	expireAt := time.Now().Add(time.Second)
	result := ed.Expire("key1", expireAt)
	if !result {
		t.Error("Expire() should return true for existing key")
	}
	
	// 检查TTL
	ttl := ed.TTL("key1")
	if ttl < 0 || ttl > 1 {
		t.Errorf("TTL() = %d, want between 0 and 1", ttl)
	}
	
	// 对不存在的key设置过期时间
	result = ed.Expire("nonexistent", expireAt)
	if result {
		t.Error("Expire() should return false for non-existent key")
	}
}

func TestExpireDict_Persist(t *testing.T) {
	ed := NewExpireDict(16)
	
	// 设置带过期时间的字段
	ed.SetWithExpire("key1", "value1", time.Second)
	
	// 移除过期时间
	result := ed.Persist("key1")
	if !result {
		t.Error("Persist() should return true for key with expiration")
	}
	
	// 检查TTL应该为-1（无过期时间）
	ttl := ed.TTL("key1")
	if ttl != -1 {
		t.Errorf("TTL() = %d, want -1 (no expiration)", ttl)
	}
	
	// 再次调用Persist应该返回false
	result = ed.Persist("key1")
	if result {
		t.Error("Persist() should return false for key without expiration")
	}
}

func TestExpireDict_Delete(t *testing.T) {
	ed := NewExpireDict(16)
	
	ed.SetWithExpire("key1", "value1", time.Second)
	
	if ed.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", ed.Len())
	}
	
	result := ed.Delete("key1")
	if result != 1 {
		t.Errorf("Delete() = %d, want 1", result)
	}
	
	if ed.Len() != 0 {
		t.Errorf("Len() = %d, want 0", ed.Len())
	}
}

func TestExpireDict_Len(t *testing.T) {
	ed := NewExpireDict(16)
	
	if ed.Len() != 0 {
		t.Errorf("Len() = %d, want 0", ed.Len())
	}
	
	ed.Set("key1", "value1")
	ed.Set("key2", "value2")
	ed.SetWithExpire("key3", "value3", time.Second)
	
	if ed.Len() != 3 {
		t.Errorf("Len() = %d, want 3", ed.Len())
	}
}

func TestExpireDict_ForEach(t *testing.T) {
	ed := NewExpireDict(16)
	
	ed.Set("key1", "value1")
	ed.Set("key2", "value2")
	
	count := 0
	ed.ForEach(func(key string, val interface{}) bool {
		count++
		return true
	})
	
	if count != 2 {
		t.Errorf("ForEach visited %d items, want 2", count)
	}
}

func TestExpireDict_TTL(t *testing.T) {
	ed := NewExpireDict(16)
	
	// 不存在的key
	ttl := ed.TTL("nonexistent")
	if ttl != -2 {
		t.Errorf("TTL() for nonexistent = %d, want -2", ttl)
	}
	
	// 没有过期时间的key
	ed.Set("key1", "value1")
	ttl = ed.TTL("key1")
	if ttl != -1 {
		t.Errorf("TTL() for no expire = %d, want -1", ttl)
	}
}

func TestExpireDict_PTTL(t *testing.T) {
	ed := NewExpireDict(16)
	
	ed.SetWithExpire("key1", "value1", time.Second)
	
	pttl := ed.PTTL("key1")
	if pttl < 0 || pttl > 1000 {
		t.Errorf("PTTL() = %d, want between 0 and 1000", pttl)
	}
}

func TestExpireDict_GetWithExpire(t *testing.T) {
	ed := NewExpireDict(16)
	
	// 设置带过期时间的字段
	ed.SetWithExpire("key1", "value1", time.Second)
	
	val, ttl, exists := ed.GetWithExpire("key1")
	if !exists {
		t.Error("GetWithExpire() should return exists=true")
	}
	if val != "value1" {
		t.Errorf("GetWithExpire() val = %v, want value1", val)
	}
	if ttl < 0 || ttl > time.Second {
		t.Errorf("GetWithExpire() ttl = %v, want between 0 and 1s", ttl)
	}
	
	// 不存在的key
	_, ttl, exists = ed.GetWithExpire("nonexistent")
	if exists {
		t.Error("GetWithExpire() for nonexistent should return exists=false")
	}
	if ttl != -2 {
		t.Errorf("GetWithExpire() for nonexistent ttl = %v, want -2", ttl)
	}
}
