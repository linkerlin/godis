package dict

import (
	"math/rand"
	"sort"

	"github.com/linkerlin/godis/lib/wildcard"
)

// SimpleDict wraps a map, it is not thread safe
type SimpleDict struct {
	m map[string]interface{}
}

// MakeSimple makes a new map
func MakeSimple() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

// Get returns the binding value and whether the key is exist
func (dict *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m[key]
	return val, ok
}

// Len returns the number of dict
func (dict *SimpleDict) Len() int {
	if dict.m == nil {
		panic("m is nil")
	}
	return len(dict.m)
}

// Put puts key value into dict and returns the number of new inserted key-value
func (dict *SimpleDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	dict.m[key] = val
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	if existed {
		return 0
	}
	dict.m[key] = val
	return 1
}

// PutIfExists puts value if the key is existed and returns the number of inserted key-value
func (dict *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m[key]
	if existed {
		dict.m[key] = val
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *SimpleDict) Remove(key string) (val interface{}, result int) {
	val, existed := dict.m[key]
	delete(dict.m, key)
	if existed {
		return val, 1
	}
	return nil, 0
}

// Keys returns all keys in dict
func (dict *SimpleDict) Keys() []string {
	result := make([]string, len(dict.m))
	i := 0
	for k := range dict.m {
		result[i] = k
		i++
	}
	return result
}

// ForEach traversal the dict
func (dict *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range dict.m {
		if !consumer(k, v) {
			break
		}
	}
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *SimpleDict) RandomKeys(limit int) []string {
	if limit <= 0 || len(dict.m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(dict.m))
	for k := range dict.m {
		keys = append(keys, k)
	}
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		result[i] = keys[rand.Intn(len(keys))]
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *SimpleDict) RandomDistinctKeys(limit int) []string {
	if limit <= 0 || len(dict.m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(dict.m))
	for k := range dict.m {
		keys = append(keys, k)
	}
	if limit > len(keys) {
		limit = len(keys)
	}
	for i := 0; i < limit; i++ {
		j := i + rand.Intn(len(keys)-i)
		keys[i], keys[j] = keys[j], keys[i]
	}
	return keys[:limit]
}

// Clear removes all keys in dict
func (dict *SimpleDict) Clear() {
	*dict = *MakeSimple()
}

func (dict *SimpleDict) DictScan(cursor int, count int, pattern string) ([][]byte, int) {
	result := make([][]byte, 0)
	matchKey, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return result, -1
	}
	if count <= 0 {
		count = 10
	}
	keys := make([]string, 0, len(dict.m))
	for k := range dict.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if cursor < 0 || cursor > len(keys) {
		return result, 0
	}
	i := cursor
	visited := 0
	for i < len(keys) && visited < count {
		k := keys[i]
		i++
		visited++
		if pattern != "*" && !matchKey.IsMatch(k) {
			continue
		}
		raw, exists := dict.Get(k)
		if !exists {
			continue
		}
		val, ok := raw.([]byte)
		if !ok {
			continue
		}
		result = append(result, []byte(k), val)
	}
	next := i
	if next >= len(keys) {
		next = 0
	}
	return result, next
}
