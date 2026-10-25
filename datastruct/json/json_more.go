package json

import (
	"fmt"
	"sort"
	"strings"
)

// DelRecursive deletes every node selected by a recursive `$` path,
// returning the number removed (objects via swap-delete; array removals
// batched per slice in descending index order).
func (jv *JSONValue) DelRecursive(path string) int {
	type arrDel struct {
		slot jsonSlot
		idx  int
	}
	groups := map[interface{}][]arrDel{}
	var order []interface{}
	count := 0
	arrKey := func(a []interface{}) interface{} {
		if len(a) == 0 {
			return fmt.Sprintf("empty-%p", &a)
		}
		return &a[0]
	}
	for _, s := range resolveSlots(jv.data, path) {
		if s.idx >= 0 {
			arr, ok := s.get().([]interface{})
			if !ok {
				continue
			}
			key := arrKey(arr)
			if _, seen := groups[key]; !seen {
				order = append(order, key)
			}
			groups[key] = append(groups[key], arrDel{slot: s, idx: s.idx})
			continue
		}
		if s.rm != nil {
			s.rm()
			count++
		}
	}
	for _, key := range order {
		dels := groups[key]
		sort.Slice(dels, func(i, j int) bool { return dels[i].idx > dels[j].idx })
		cur, _ := dels[0].slot.get().([]interface{})
		for _, d := range dels {
			if d.idx >= 0 && d.idx < len(cur) {
				cur = append(cur[:d.idx], cur[d.idx+1:]...)
				count++
			}
		}
		dels[0].slot.set(cur)
	}
	return count
}

// StrLenMatches returns per-match string lengths (nil for non-strings).
func (jv *JSONValue) StrLenMatches(path string) []interface{} {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		if str, ok := s.get().(string); ok {
			out = append(out, int64(len([]rune(str))))
		} else {
			out = append(out, nil)
		}
	}
	return out
}

// ArrLenMatches returns per-match array lengths (nil for non-arrays).
func (jv *JSONValue) ArrLenMatches(path string) []interface{} {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		if arr, ok := s.get().([]interface{}); ok {
			out = append(out, int64(len(arr)))
		} else {
			out = append(out, nil)
		}
	}
	return out
}

// ArrAppendMatches appends values to every selected array, returning the
// per-match new lengths (nil for non-arrays).
func (jv *JSONValue) ArrAppendMatches(path string, values ...interface{}) []interface{} {
	jv.mu.Lock()
	defer jv.mu.Unlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		arr, ok := s.get().([]interface{})
		if !ok {
			out = append(out, nil)
			continue
		}
		na := append(append([]interface{}{}, arr...), values...)
		s.set(na)
		out = append(out, int64(len(na)))
	}
	return out
}

// ArrPopMatches pops one element from every selected array (nil for
// non-arrays and empty/out-of-range pops).
func (jv *JSONValue) ArrPopMatches(path string, index int) []interface{} {
	jv.mu.Lock()
	defer jv.mu.Unlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		arr, ok := s.get().([]interface{})
		if !ok || len(arr) == 0 {
			out = append(out, nil)
			continue
		}
		idx := index
		if idx < 0 {
			idx = len(arr) + idx
		}
		if idx < 0 || idx >= len(arr) {
			out = append(out, nil)
			continue
		}
		elem := arr[idx]
		na := append(append([]interface{}{}, arr[:idx]...), arr[idx+1:]...)
		s.set(na)
		out = append(out, elem)
	}
	return out
}

// ArrIndexMatches scans every selected array for value (string form),
// returning per-match positions (nil for non-arrays).
func (jv *JSONValue) ArrIndexMatches(path string, value string, start, stop int) []interface{} {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		arr, ok := s.get().([]interface{})
		if !ok {
			out = append(out, nil)
			continue
		}
		if start < 0 {
			start = 0
		}
		if stop < 0 || stop >= len(arr) {
			stop = len(arr) - 1
		}
		pos := int64(-1)
		for i := start; i <= stop && i < len(arr); i++ {
			if fmt.Sprintf("%v", arr[i]) == value {
				pos = int64(i)
				break
			}
		}
		out = append(out, pos)
	}
	return out
}

// IsLegacyJSONPath reports whether path uses the legacy (non-`$`) form.
func IsLegacyJSONPath(path string) bool {
	return !strings.HasPrefix(path, "$")
}

// ArrTrim trims an array to the specified range
func (jv *JSONValue) ArrTrim(path string, start, stop int) (int, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()
	
	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	
	arr, ok := val.([]interface{})
	if !ok {
		return 0, fmt.Errorf("value at path is not an array")
	}
	
	if start < 0 {
		start = 0
	}
	if stop >= len(arr) || stop < 0 {
		stop = len(arr) - 1
	}
	if start > stop {
		arr = []interface{}{}
	} else {
		arr = arr[start : stop+1]
	}
	
	jv.setPath(path, arr, false, false)
	return len(arr), nil
}

// ArrIndex returns the index of the first occurrence of value in array
func (jv *JSONValue) ArrIndex(path string, value string, start, stop int) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	
	val, err := jv.getPath(path)
	if err != nil {
		return -1, err
	}
	
	arr, ok := val.([]interface{})
	if !ok {
		return -1, fmt.Errorf("value at path is not an array")
	}
	
	if start < 0 {
		start = 0
	}
	if stop < 0 || stop >= len(arr) {
		stop = len(arr) - 1
	}
	
	for i := start; i <= stop && i < len(arr); i++ {
		if fmt.Sprintf("%v", arr[i]) == value {
			return i, nil
		}
	}
	
	return -1, nil
}

// MarshalValue serializes any stored JSON value (ordered objects, int64
// integers, whole-float `.0` forms) exactly as stored documents render.
func MarshalValue(v interface{}) ([]byte, error) {
	return marshalOrderedValue(v)
}

// ObjKeysMatches concatenates the keys of every selected object in document
// order; ok=false when no object matched (real replies null then).
func (jv *JSONValue) ObjKeysMatches(path string) ([]string, bool) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	var keys []string
	for _, s := range resolveSlots(jv.data, path) {
		if ks, ok := orderedKeys(s.get()); ok {
			keys = append(keys, ks...)
		}
	}
	return keys, keys != nil
}

// StrAppendMatches appends suffix to every selected string, returning the
// per-match new lengths (nil for non-strings).
func (jv *JSONValue) StrAppendMatches(path string, suffix string) []interface{} {
	jv.mu.Lock()
	defer jv.mu.Unlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		if str, ok := s.get().(string); ok {
			ns := str + suffix
			s.set(ns)
			out = append(out, int64(len([]rune(ns))))
		} else {
			out = append(out, nil)
		}
	}
	return out
}

// ObjLenMatches returns per-match object key counts (nil for non-objects).
func (jv *JSONValue) ObjLenMatches(path string) []interface{} {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	out := make([]interface{}, 0, 4)
	for _, s := range resolveSlots(jv.data, path) {
		if obj, ok := ObjAsOrdered(s.get()); ok {
			out = append(out, int64(obj.Len()))
		} else {
			out = append(out, nil)
		}
	}
	return out
}
