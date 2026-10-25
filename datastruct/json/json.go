package json

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// JSONValue represents a JSON value stored in Redis
type JSONValue struct {
	data interface{}
	mu   sync.RWMutex
}

// Data returns the underlying value (ordered objects, int64/float64).
func (jv *JSONValue) Data() interface{} {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	return jv.data
}

// NewJSONValue creates a new JSON value from a Go interface{}
func NewJSONValue(data interface{}) *JSONValue {
	return &JSONValue{data: data}
}

// NewJSONValueFromBytes creates a new JSON value from JSON bytes,
// preserving document key order and the integer/number distinction.
func NewJSONValueFromBytes(data []byte) (*JSONValue, error) {
	v, err := parseTopLevelJSON(data)
	if err != nil {
		return nil, err
	}
	return &JSONValue{data: v}, nil
}

// parseTopLevelJSON decodes any JSON value (not only objects).
func parseTopLevelJSON(data []byte) (interface{}, error) {
	// Ordered objects need the ordered decoder; scalars/arrays go through
	// parseOrderedValue on the raw bytes.
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '{' {
		obj := NewOrderedObj()
		if err := obj.UnmarshalJSON(data); err != nil {
			return nil, err
		}
		return obj, nil
	}
	return parseOrderedValue(json.RawMessage(trimmed))
}

// NewJSONValueFromString creates a new JSON value from a JSON string
func NewJSONValueFromString(s string) (*JSONValue, error) {
	return NewJSONValueFromBytes([]byte(s))
}

// convertNumbers converts json.Number to float64 for easier manipulation
func convertNumbers(v interface{}) interface{} {
	switch val := v.(type) {
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f
		}
		return val
	case map[string]interface{}:
		for k, v := range val {
			val[k] = convertNumbers(v)
		}
		return val
	case []interface{}:
		for i, v := range val {
			val[i] = convertNumbers(v)
		}
		return val
	default:
		return v
	}
}

// Get returns the value at the given JSON path
func (jv *JSONValue) Get(path string) (interface{}, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	if path == "$" || path == "." || path == "" {
		return jv.data, nil
	}

	return jv.getPath(path)
}

// Set sets a value at the given JSON path
func (jv *JSONValue) Set(path string, value interface{}, nx bool, xx bool) (bool, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	if path == "$" || path == "." || path == "" {
		if xx && jv.data != nil {
			return true, nil // Already exists, don't change
		}
		if nx && jv.data != nil {
			return false, nil // Already exists, don't set
		}
		jv.data = value
		return true, nil
	}

	return jv.setPath(path, value, nx, xx)
}

// Del deletes a value at the given JSON path
func (jv *JSONValue) Del(path string) (bool, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	if path == "$" || path == "." || path == "" {
		jv.data = nil
		return true, nil
	}

	return jv.delPath(path)
}

// Type returns the type of the value at the given path
func (jv *JSONValue) Type(path string) (string, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	val, err := jv.getPath(path)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "null", nil
	}

	switch val.(type) {
	case *OrderedObj:
		return "object", nil
	case []interface{}:
		return "array", nil
	case string:
		return "string", nil
	case int64, int:
		return "integer", nil
	case float64, float32:
		return "number", nil
	case bool:
		return "boolean", nil
	default:
		return "unknown", nil
	}
}

// NumIncrBy increments every number at path by delta. Real 8.10: matches
// reply as one JSON-serialized array (int preserved for int+int, `.0` for
// whole floats, null for non-numbers); a legacy path keeps the single-value
// error text.
func (jv *JSONValue) NumIncrBy(path string, increment float64) ([]interface{}, error) {
	return jv.numOp(path, increment, false)
}

// NumMultBy multiplies every number at path by factor.
func (jv *JSONValue) NumMultBy(path string, multiplier float64) ([]interface{}, error) {
	return jv.numOp(path, multiplier, true)
}

func (jv *JSONValue) numOp(path string, delta float64, mult bool) ([]interface{}, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	apply := func(v interface{}) (interface{}, bool) {
		// Real RedisJSON: integer op integer stays an integer; anything
		// else is double arithmetic (probed: 1+5 -> 6 not 6.0).
		if iv, ok := v.(int64); ok && delta == float64(int64(delta)) {
			if mult {
				return iv * int64(delta), true
			}
			return iv + int64(delta), true
		}
		num, ok := numericValue(v)
		if !ok {
			return nil, false
		}
		if mult {
			return num * delta, true
		}
		return num + delta, true
	}

	if !strings.HasPrefix(path, "$") {
		val, err := jv.getPath(path)
		if err != nil {
			return nil, fmt.Errorf("ERR Path does not exist or does not contains a number")
		}
		newVal, ok := apply(val)
		if !ok {
			return nil, fmt.Errorf("ERR Path does not exist or does not contains a number")
		}
		jv.setPath(path, newVal, false, false)
		return []interface{}{newVal}, nil
	}

	slots := resolveSlots(jv.data, path)
	out := make([]interface{}, 0, len(slots))
	for _, s := range slots {
		newVal, ok := apply(s.get())
		if !ok {
			out = append(out, nil)
			continue
		}
		s.set(newVal)
		out = append(out, newVal)
	}
	return out, nil
}

// numericValue extracts a float from a JSON number value.
func numericValue(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

// NumMultBy multiplies (replaced by numOp; see NumIncrBy).

// StrLen returns the length of a string at the given path
func (jv *JSONValue) StrLen(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	str, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE wrong type of path value - expected string")
	}
	return len(str), nil
}

// Clear resets a container/scalar at path to empty ({} / [] / "" / 0 / false).
// Returns 1 if a value was cleared, 0 if already empty / missing.
func (jv *JSONValue) Clear(path string) (int, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, nil
	}
	switch v := val.(type) {
	case *OrderedObj:
		if v.Len() == 0 {
			return 0, nil
		}
		jv.setPath(path, NewOrderedObj(), false, false)
		return 1, nil
	case map[string]interface{}:
		if len(v) == 0 {
			return 0, nil
		}
		jv.setPath(path, NewOrderedObj(), false, false)
		return 1, nil
	case []interface{}:
		if len(v) == 0 {
			return 0, nil
		}
		jv.setPath(path, []interface{}{}, false, false)
		return 1, nil
	case float64, float32, int, int64:
		jv.setPath(path, int64(0), false, false)
		return 1, nil
	}
	// Real 8.10: CLEAR empties containers and zeroes numbers; strings and
	// booleans are left untouched (probed).
	return 0, nil
}

// Toggle flips every boolean at path. Real 8.10 contracts (probed): a legacy
// path toggles its single value, replying the new value as JSON text, and
// errors "ERR Path does not exist or not a bool" on a miss or non-bool; a `$`
// path replies per match in selection order — integer 1/0 (new value) for
// booleans, nil for non-boolean nodes, empty array for zero matches.
func (jv *JSONValue) Toggle(path string) ([]interface{}, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	if !strings.HasPrefix(path, "$") {
		val, err := jv.getPath(path)
		if err != nil {
			return nil, fmt.Errorf("ERR Path does not exist or not a bool")
		}
		b, ok := val.(bool)
		if !ok {
			return nil, fmt.Errorf("ERR Path does not exist or not a bool")
		}
		jv.setPath(path, !b, false, false)
		return []interface{}{!b}, nil
	}

	out := make([]interface{}, 0)
	if normalizeJSONPath(path) == "" {
		if b, ok := jv.data.(bool); ok {
			jv.data = !b
			if b {
				out = append(out, int64(0))
			} else {
				out = append(out, int64(1))
			}
		} else {
			out = append(out, nil)
		}
		return out, nil
	}
	finishToggles(resolveSlots(jv.data, path), &out)
	return out, nil
}

// resolveSlots maps a `$` path grammar onto writable slots, one per match,
// in document order (plain and recursive descent paths).
func resolveSlots(root interface{}, path string) []jsonSlot {
	norm := normalizeJSONPath(path)
	if norm == "" {
		return []jsonSlot{deadSlot(root)}
	}
	if idx := recursiveDescentIndex(norm); idx >= 0 {
		prefix, suffix := norm[:idx], norm[idx+2:]
		var base []jsonSlot
		if prefix != "" {
			base = stepSlots([]jsonSlot{deadSlot(root)}, parsePath(prefix))
		} else {
			base = []jsonSlot{deadSlot(root)}
		}
		parts := parsePath(suffix)
		if len(parts) == 0 {
			return base
		}
		collected := make([]jsonSlot, 0)
		switch {
		case parts[0] == "*":
			for _, s := range base {
				collected = descendantSlots(s.get(), collected)
			}
		case strings.HasPrefix(suffix, "[") && isNumericPart(parts[0]):
			n := atoiPart(parts[0])
			for _, s := range base {
				collected = arrayElemSlots(s.get(), n, collected)
			}
		default:
			for _, s := range base {
				collected = recursiveKeySlots(s.get(), parts[0], collected)
			}
		}
		return stepSlots(collected, parts[1:])
	}
	return stepSlots([]jsonSlot{deadSlot(root)}, parsePath(norm))
}

// jsonSlot is a writable document position: get/set address one container
// member or array element; rm removes it (objects delete immediately with
// swap semantics; array removals are deferred to the caller, which uses idx
// to batch them per slice).
type jsonSlot struct {
	get func() interface{}
	set func(interface{})
	rm  func() // nil for dead slots and array elements
	idx int    // array index when this slot addresses one (-1 otherwise)
}

// deadSlot selects a value without write access (e.g. the root document or
// `~` key names); a toggle landing there flips nothing.
func deadSlot(v interface{}) jsonSlot {
	return jsonSlot{get: func() interface{} { return v }, set: func(interface{}) {}, idx: -1}
}

func objSlot(o *OrderedObj, key string) jsonSlot {
	return jsonSlot{
		get: func() interface{} { v, _ := o.Get(key); return v },
		set: func(v interface{}) { o.m[key] = v },
		rm:  func() { o.Delete(key) },
		idx: -1,
	}
}

func mapSlot(m map[string]interface{}, key string) jsonSlot {
	return jsonSlot{
		get: func() interface{} { return m[key] },
		set: func(v interface{}) { m[key] = v },
		rm:  func() { delete(m, key) },
		idx: -1,
	}
}

func arrSlot(a []interface{}, idx int) jsonSlot {
	return jsonSlot{
		get: func() interface{} { return a[idx] },
		set: func(v interface{}) { a[idx] = v },
		idx: idx,
	}
}

// stepSlots advances slots through path parts (the write-back twin of
// applyNodeSteps' selection loop).
func stepSlots(slots []jsonSlot, parts []string) []jsonSlot {
	for _, part := range parts {
		if part == "" || part == "$" || part == "@" {
			continue
		}
		next := make([]jsonSlot, 0, len(slots))
		switch {
		case part == "*":
			for _, s := range slots {
				switch c := s.get().(type) {
				case *OrderedObj:
					for _, k := range c.Keys() {
						next = append(next, objSlot(c, k))
					}
				case map[string]interface{}:
					for _, k := range sortedStringKeys(c) {
						next = append(next, mapSlot(c, k))
					}
				case []interface{}:
					for i := range c {
						next = append(next, arrSlot(c, i))
					}
				}
			}
		case IsFilterPart(part):
			for _, s := range slots {
				if arr, ok := s.get().([]interface{}); ok {
					if idxs, err := ApplyFilterIndices(arr, part); err == nil {
						for _, i := range idxs {
							next = append(next, arrSlot(arr, i))
						}
					}
				}
			}
		case part == "~":
			for _, s := range slots {
				if keys, ok := orderedKeys(s.get()); ok {
					for range keys {
						next = append(next, deadSlot(nil))
					}
				}
			}
		default:
			for _, s := range slots {
				switch c := s.get().(type) {
				case *OrderedObj:
					if _, ok := c.Get(part); ok {
						next = append(next, objSlot(c, part))
					}
				case map[string]interface{}:
					if _, ok := c[part]; ok {
						next = append(next, mapSlot(c, part))
					}
				case []interface{}:
					if idx, err := strconv.Atoi(part); err == nil && idx >= 0 && idx < len(c) {
						next = append(next, arrSlot(c, idx))
					}
				}
			}
		}
		slots = next
		if len(slots) == 0 {
			return nil
		}
	}
	return slots
}

// pathSlots resolves a non-recursive path to its writable slots.
func pathSlots(root interface{}, raw string) []jsonSlot {
	return stepSlots([]jsonSlot{deadSlot(root)}, parsePath(raw))
}

// finishToggles flips the booleans among slots, recording per-match results.
func finishToggles(slots []jsonSlot, out *[]interface{}) {
	for _, s := range slots {
		if b, ok := s.get().(bool); ok {
			s.set(!b)
			if b {
				*out = append(*out, int64(0))
			} else {
				*out = append(*out, int64(1))
			}
		} else {
			*out = append(*out, nil)
		}
	}
}

// recursiveKeySlots mirrors collectRecursiveKey (pre-order) with write-back.
func recursiveKeySlots(node interface{}, key string, out []jsonSlot) []jsonSlot {
	if m, ok := node.(map[string]interface{}); ok {
		for _, k := range sortedStringKeys(m) {
			if k == key {
				out = append(out, mapSlot(m, k))
			}
			out = recursiveKeySlots(m[k], key, out)
		}
		return out
	}
	switch t := node.(type) {
	case *OrderedObj:
		for _, k := range t.Keys() {
			if k == key {
				out = append(out, objSlot(t, k))
			}
			v, _ := t.Get(k)
			out = recursiveKeySlots(v, key, out)
		}
	case []interface{}:
		for _, v := range t {
			out = recursiveKeySlots(v, key, out)
		}
	}
	return out
}

// descendantSlots mirrors collectDescendants (children-first) with write-back.
func descendantSlots(node interface{}, out []jsonSlot) []jsonSlot {
	if m, ok := node.(map[string]interface{}); ok {
		kids := make([]interface{}, 0, len(m))
		for _, k := range sortedStringKeys(m) {
			kids = append(kids, m[k])
		}
		for _, k := range sortedStringKeys(m) {
			out = append(out, mapSlot(m, k))
		}
		for _, k := range kids {
			out = descendantSlots(k, out)
		}
		return out
	}
	switch t := node.(type) {
	case *OrderedObj:
		for _, k := range t.Keys() {
			out = append(out, objSlot(t, k))
		}
		for _, v := range t.Values() {
			out = descendantSlots(v, out)
		}
	case []interface{}:
		for i := range t {
			out = append(out, arrSlot(t, i))
		}
		for _, e := range t {
			out = descendantSlots(e, out)
		}
	}
	return out
}

// arrayElemSlots mirrors collectArrayElem (every descendant array's idx-th
// element) with write-back.
func arrayElemSlots(node interface{}, idx int, out []jsonSlot) []jsonSlot {
	if m, ok := node.(map[string]interface{}); ok {
		for _, k := range sortedStringKeys(m) {
			out = arrayElemSlots(m[k], idx, out)
		}
		return out
	}
	switch t := node.(type) {
	case *OrderedObj:
		for _, v := range t.Values() {
			out = arrayElemSlots(v, idx, out)
		}
	case []interface{}:
		if idx >= 0 && idx < len(t) {
			out = append(out, arrSlot(t, idx))
		}
		for _, e := range t {
			out = arrayElemSlots(e, idx, out)
		}
	}
	return out
}

// Merge applies RFC7396 JSON Merge Patch at path.
func (jv *JSONValue) Merge(path string, patch interface{}) error {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	if path == "$" || path == "." || path == "" {
		jv.data = mergeJSON(jv.data, patch)
		return nil
	}
	cur, err := jv.getPath(path)
	if err != nil {
		// create path with patch
		_, err2 := jv.setPath(path, patch, false, false)
		return err2
	}
	merged := mergeJSON(cur, patch)
	_, err = jv.setPath(path, merged, false, false)
	return err
}

func mergeJSON(target, patch interface{}) interface{} {
	if patch == nil {
		return nil
	}
	patchKeys, patchIsObj := objKeysOf(patch)
	if !patchIsObj {
		return patch
	}
	var targetObj *OrderedObj
	if o, ok := ObjAsOrdered(target); ok {
		// shallow copy to avoid mutating shared structures unexpectedly
		cp := NewOrderedObj()
		for _, k := range o.Keys() {
			v, _ := o.Get(k)
			cp.Set(k, v)
		}
		targetObj = cp
	} else if m, ok := target.(map[string]interface{}); ok {
		cp := NewOrderedObj()
		for k, v := range m {
			cp.Set(k, v)
		}
		targetObj = cp
	} else {
		targetObj = NewOrderedObj()
	}
	for _, k := range patchKeys {
		v := objMemberOf(patch, k)
		if v == nil {
			targetObj.Delete(k)
			continue
		}
		existing, _ := targetObj.Get(k)
		targetObj.Set(k, mergeJSON(existing, v))
	}
	return targetObj
}

// objKeysOf returns ordered keys for either object representation.
func objKeysOf(v interface{}) ([]string, bool) {
	switch t := v.(type) {
	case *OrderedObj:
		return t.Keys(), true
	case map[string]interface{}:
		return sortedStringKeys(t), true
	}
	return nil, false
}

// objMemberOf reads a member from either object representation.
func objMemberOf(v interface{}, k string) interface{} {
	switch t := v.(type) {
	case *OrderedObj:
		val, _ := t.Get(k)
		return val
	case map[string]interface{}:
		return t[k]
	}
	return nil
}

// DebugMemory returns approximate serialized byte size at path.
func (jv *JSONValue) DebugMemory(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	b, err := json.Marshal(val)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

// DebugFields counts scalar leaf values under path.
func (jv *JSONValue) DebugFields(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	return countJSONFields(val), nil
}

func countJSONFields(v interface{}) int {
	switch val := v.(type) {
	case map[string]interface{}:
		n := 0
		for _, child := range val {
			n += countJSONFields(child)
		}
		return n
	case []interface{}:
		n := 0
		for _, child := range val {
			n += countJSONFields(child)
		}
		return n
	default:
		return 1
	}
}

// StrAppend appends a string to the value at the given path
func (jv *JSONValue) StrAppend(path string, appendStr string) (int, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}

	str, ok := val.(string)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE wrong type of path value - expected string")
	}

	newStr := str + appendStr
	jv.setPath(path, newStr, false, false)
	return len(newStr), nil
}

// ArrAppend appends values to an array at the given path
func (jv *JSONValue) ArrAppend(path string, values ...interface{}) (int, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, fmt.Errorf("ERR Path does not exist or not an array")
	}

	arr = append(arr, values...)
	jv.setPath(path, arr, false, false)
	return len(arr), nil
}

// ArrInsert inserts values into an array at the given index
func (jv *JSONValue) ArrInsert(path string, index int, values ...interface{}) (int, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, fmt.Errorf("ERR Path does not exist or not an array")
	}

	if index < 0 || index > len(arr) {
		return 0, fmt.Errorf("index out of bounds")
	}

	arr = append(arr[:index], append(values, arr[index:]...)...)
	jv.setPath(path, arr, false, false)
	return len(arr), nil
}

// ArrPop removes and returns an element from an array at the given index
func (jv *JSONValue) ArrPop(path string, index int) (interface{}, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()

	val, err := jv.getPath(path)
	if err != nil {
		return nil, err
	}

	arr, ok := val.([]interface{})
	if !ok {
		return nil, fmt.Errorf("ERR Path does not exist or not an array")
	}

	if len(arr) == 0 {
		return nil, fmt.Errorf("array is empty")
	}

	if index < 0 {
		index = len(arr) + index
	}

	if index < 0 || index >= len(arr) {
		return nil, fmt.Errorf("index out of bounds")
	}

	elem := arr[index]
	arr = append(arr[:index], arr[index+1:]...)
	jv.setPath(path, arr, false, false)
	return elem, nil
}

// ArrLen returns the length of an array at the given path
func (jv *JSONValue) ArrLen(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}

	arr, ok := val.([]interface{})
	if !ok {
		return 0, fmt.Errorf("ERR Path does not exist or not an array")
	}

	return len(arr), nil
}

// ObjKeys returns the keys of an object at the given path
func (jv *JSONValue) ObjKeys(path string) ([]string, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	val, err := jv.getPath(path)
	if err != nil {
		return nil, err
	}

	obj, ok := ObjAsOrdered(val)
	if !ok {
		return nil, fmt.Errorf("ERR Path does not exist or not an object")
	}

	return obj.Keys(), nil
}

// ObjLen returns the number of keys in an object at the given path
func (jv *JSONValue) ObjLen(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}

	obj, ok := ObjAsOrdered(val)
	if !ok {
		return 0, fmt.Errorf("WRONGTYPE wrong type of path value - expected object")
	}

	return obj.Len(), nil
}

// ToBytes returns the JSON value as bytes
func (jv *JSONValue) ToBytes() ([]byte, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()

	return marshalOrderedValue(jv.data)
}

// ToString returns the JSON value as a string
func (jv *JSONValue) ToString() (string, error) {
	b, err := jv.ToBytes()
	return string(b), err
}

// PathHasNodelist reports whether a path contains a wildcard (`[*]`, `.*`) or
// recursive-descent (`..`) segment that yields a multi-value nodelist.
func PathHasNodelist(path string) bool {
	if hasRecursiveDescent(path) {
		return true
	}
	for _, part := range parsePath(path) {
		if part == "*" || part == "~" {
			return true
		}
	}
	return false
}

func hasRecursiveDescent(path string) bool {
	return recursiveDescentIndex(path) >= 0
}

func recursiveDescentIndex(path string) int {
	depth := 0
	quote := byte(0)
	for i := 0; i+1 < len(path); i++ {
		c := path[i]
		if quote != 0 {
			if c == '\\' {
				i++
				continue
			}
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		case '.':
			if depth == 0 && path[i+1] == '.' {
				return i
			}
		}
	}
	return -1
}

func normalizeJSONPath(raw string) string {
	p := strings.TrimSpace(raw)
	if strings.HasPrefix(p, "$") || strings.HasPrefix(p, "@") {
		p = p[1:]
	}
	if strings.HasPrefix(p, ".") && !strings.HasPrefix(p, "..") {
		p = p[1:]
	}
	return p
}

// evalPathNodes evaluates a path that contains wildcards or recursive descent
// and returns all matched values in document order (object keys sorted).
func evalPathNodes(root interface{}, raw string) ([]interface{}, error) {
	path := normalizeJSONPath(raw)
	if path == "" {
		return []interface{}{root}, nil
	}
	if idx := recursiveDescentIndex(path); idx >= 0 {
		prefix, suffix := path[:idx], path[idx+2:]
		base := []interface{}{root}
		if prefix != "" {
			vals, err := evalPathNodes(root, prefix)
			if err != nil {
				return nil, err
			}
			base = vals
		}
		parts := parsePath(suffix)
		if len(parts) == 0 {
			return base, nil
		}
		collected := make([]interface{}, 0)
		switch {
		case parts[0] == "*":
			for _, node := range base {
				collected = collectDescendants(node, collected)
			}
		case strings.HasPrefix(suffix, "[") && isNumericPart(parts[0]):
			// `$..[N]` — the Nth element of EVERY descendant array
			// (the arrays themselves included), in document order.
			// (parsePath strips the brackets, so the raw suffix decides.)
			idx := atoiPart(parts[0])
			for _, node := range base {
				collected = collectArrayElem(node, idx, collected)
			}
		default:
			for _, node := range base {
				collected = collectRecursiveKey(node, parts[0], collected)
			}
		}
		return applyNodeSteps(collected, parts[1:]), nil
	}
	return applyNodeSteps([]interface{}{root}, parsePath(path)), nil
}

func applyNodeSteps(nodes []interface{}, parts []string) []interface{} {
	for _, part := range parts {
		if part == "" || part == "@" || part == "$" {
			continue
		}
		next := make([]interface{}, 0)
		switch {
		case part == "*":
			for _, node := range nodes {
				next = append(next, wildcardValues(node)...)
			}
		case IsFilterPart(part):
			for _, node := range nodes {
				if arr, ok := node.([]interface{}); ok {
					if matches, err := ApplyFilter(arr, part); err == nil {
						next = append(next, matches...)
					}
				}
			}
		case part == "~":
			// Property names, flattened across all selected nodes
			// (real: `$.*~` on root {nums:{},obj:{b,a}} -> ["b","a"]).
			for _, node := range nodes {
				if obj, ok := ObjAsOrdered(node); ok {
					for _, k := range obj.Keys() {
						next = append(next, k)
					}
				} else if m, ok := node.(map[string]interface{}); ok {
					for _, k := range sortedStringKeys(m) {
						next = append(next, k)
					}
				}
			}
		default:
			for _, node := range nodes {
				if v, err := walkParts(node, []string{part}); err == nil {
					next = append(next, v)
				}
			}
		}
		nodes = next
		if len(nodes) == 0 {
			return []interface{}{}
		}
	}
	if nodes == nil {
		return []interface{}{}
	}
	return nodes
}

func wildcardValues(node interface{}) []interface{} {
	switch t := node.(type) {
	case []interface{}:
		return t
	case *OrderedObj:
		return t.Values()
	}
	return nil
}

func sortedObjectKeys(obj *OrderedObj) []interface{} {
	keys := obj.Keys()
	out := make([]interface{}, len(keys))
	for i, k := range keys {
		out[i] = k
	}
	return out
}

func sortedStringKeys(obj map[string]interface{}) []string {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// orderedKeys returns insertion-order keys for ordered objects (document
// order — real RedisJSON semantics) and sorted keys for legacy maps.
func orderedKeys(node interface{}) ([]string, bool) {
	if o, ok := ObjAsOrdered(node); ok {
		return o.Keys(), true
	}
	if m, ok := node.(map[string]interface{}); ok {
		return sortedStringKeys(m), true
	}
	return nil, false
}

func collectRecursiveKey(node interface{}, key string, out []interface{}) []interface{} {
	// Real RedisJSON descends depth-first in document order.
	if m, ok := node.(map[string]interface{}); ok {
		for _, k := range sortedStringKeys(m) {
			if k == key {
				out = append(out, m[k])
			}
			out = collectRecursiveKey(m[k], key, out)
		}
		return out
	}
	switch t := node.(type) {
	case *OrderedObj:
		for _, k := range t.Keys() {
			v, _ := t.Get(k)
			if k == key {
				out = append(out, v)
			}
			out = collectRecursiveKey(v, key, out)
		}
	case []interface{}:
		for _, v := range t {
			out = collectRecursiveKey(v, key, out)
		}
	}
	return out
}

// isNumericPart reports whether a path part is all digits.
func isNumericPart(part string) bool {
	if part == "" {
		return false
	}
	for i := 0; i < len(part); i++ {
		if part[i] < '0' || part[i] > '9' {
			return false
		}
	}
	return true
}

// atoiPart parses a decimal part (digits only).
func atoiPart(part string) int {
	n := 0
	for i := 0; i < len(part); i++ {
		n = n*10 + int(part[i]-'0')
	}
	return n
}

// isArrayIndexPart reports whether a path part is a bracketed index.
func isArrayIndexPart(part string) bool {
	if len(part) < 3 || part[0] != '[' || part[len(part)-1] != ']' {
		return false
	}
	body := part[1 : len(part)-1]
	if body == "" || body == "*" {
		return false
	}
	for i := 0; i < len(body); i++ {
		if body[i] < '0' || body[i] > '9' {
			return false
		}
	}
	return true
}

// parseIndexPart extracts the numeric index from a bracketed part.
func parseIndexPart(part string) int {
	n := 0
	for i := 1; i < len(part)-1; i++ {
		n = n*10 + int(part[i]-'0')
	}
	return n
}

// collectArrayElem appends arr[idx] for every array found depth-first
// (including node itself when it is an array).
func collectArrayElem(node interface{}, idx int, out []interface{}) []interface{} {
	if m, ok := node.(map[string]interface{}); ok {
		for _, v := range sortedStringKeys(m) {
			out = collectArrayElem(m[v], idx, out)
		}
		return out
	}
	switch t := node.(type) {
	case []interface{}:
		if idx < len(t) {
			out = append(out, t[idx])
		}
		for _, v := range t {
			out = collectArrayElem(v, idx, out)
		}
	case *OrderedObj:
		for _, k := range t.Keys() {
			v, _ := t.Get(k)
			out = collectArrayElem(v, idx, out)
		}
	}
	return out
}

func collectDescendants(node interface{}, out []interface{}) []interface{} {
	// Real RedisJSON `$..*` order (probed): every node's direct children
	// first (document order), then recurse into each child in turn.
	if m, ok := node.(map[string]interface{}); ok {
		kids := make([]interface{}, 0, len(m))
		for _, k := range sortedStringKeys(m) {
			kids = append(kids, m[k])
		}
		out = append(out, kids...)
		for _, k := range kids {
			out = collectDescendants(k, out)
		}
		return out
	}
	switch t := node.(type) {
	case *OrderedObj:
		kids := t.Values()
		out = append(out, kids...)
		for _, k := range kids {
			out = collectDescendants(k, out)
		}
	case []interface{}:
		out = append(out, t...)
		for _, e := range t {
			out = collectDescendants(e, out)
		}
	}
	return out
}

// pathGrammarError carries a verbatim wire text (no ERR prefix) that is
// still classified as a JSONPath syntax error by command handlers.
type pathGrammarError struct{ msg string }

func (e *pathGrammarError) Error() string { return e.msg }

func (e *pathGrammarError) Is(target error) bool { return target == ErrJSONPathSyntax }

// dottedTildeError reports the real-8.10 grammar error for a `.`-separated
// `~` segment: only the suffix form `$.obj~` is valid. The wire error has no
// ERR prefix and names function_name unless the `~` follows a recursive
// descent (`$..~`).
func dottedTildeError(path string) error {
	depth := 0
	quote := byte(0)
	for i := 0; i < len(path); i++ {
		c := path[i]
		if quote != 0 {
			if c == '\\' {
				i++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			if depth > 0 {
				quote = c
			}
		case '[':
			depth++
		case ']':
			if depth > 0 {
				depth--
			}
		case '~':
			if i > 0 && path[i-1] == '.' {
				fn := ", function_name"
				if i > 1 && path[i-2] == '.' {
					fn = ""
				}
				return &pathGrammarError{msg: fmt.Sprintf("Error occurred at position %d, Error at position %d: expected one of the following: <string>%s, '*'", i, i, fn)}
			}
		}
	}
	return nil
}

// getPath traverses the JSON structure following the path
func (jv *JSONValue) getPath(path string) (interface{}, error) {
	if err := dottedTildeError(path); err != nil {
		return nil, err
	}
	// Top-level projection expression (8.10 batch 3): `fn($.path)` —
	// the function applies to the single resolved value. ponytail: one
	// function over one absolute path; nested calls and wildcards are not
	// supported.
	if v, ok, err := jv.tryProjection(path); ok || err != nil {
		return v, err
	}

	// Wildcards and recursive descent yield a multi-value nodelist.
	if PathHasNodelist(path) {
		vals, err := evalPathNodes(jv.data, path)
		if err != nil {
			return nil, err
		}
		return nodeList(vals), nil
	}

	parts := parsePath(path)

	for i, part := range parts {
		// JSONPath filter segment (8.10 batch 1): applies to array
		// elements; any remaining path components project over each match
		// (misses dropped).
		if IsFilterPart(part) {
			arr, ok := jv.data.([]interface{})
			if i > 0 {
				base, err := walkParts(jv.data, parts[:i])
				if err != nil {
					return nil, err
				}
				arr, ok = base.([]interface{})
			}
			if !ok {
				return nil, ErrJSONPathSyntax
			}
			matches, err := ApplyFilter(arr, part)
			if err != nil {
				return nil, err
			}
			if i == len(parts)-1 {
				return matches, nil
			}
			out := make([]interface{}, 0, len(matches))
			for _, m := range matches {
				if v, err := walkParts(m, parts[i+1:]); err == nil {
					out = append(out, v)
				}
			}
			return out, nil
		}
	}

	return walkParts(jv.data, parts)
}

// tryProjection parses a top-level `fn($.path)` expression; ok=false for
// plain paths.
func (jv *JSONValue) tryProjection(path string) (interface{}, bool, error) {
	path = strings.TrimSpace(path)
	open := strings.IndexByte(path, '(')
	if open <= 0 || !strings.HasSuffix(path, ")") {
		return nil, false, nil
	}
	fn := path[:open]
	for i := 0; i < len(fn); i++ {
		c := fn[i]
		if !(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(i > 0 && c >= '0' && c <= '9')) {
			return nil, false, nil
		}
	}
	inner := strings.TrimSpace(path[open+1 : len(path)-1])
	if !strings.HasPrefix(inner, "$") && !strings.Contains(inner, "(") {
		return nil, false, ErrJSONPathSyntax
	}
	val, err := jv.getPath(inner)
	if err != nil {
		return nil, true, err
	}
	// Redis 8.10: length() over a wildcard/recursive nodelist yields [].
	if _, isNodes := val.(nodeList); isNodes && fn == "length" {
		return projectionResult{}, true, nil
	}
	// Nested projection: unwrap the inner one-element array before applying
	// the outer function (abs(min($.nums))).
	if pr, ok := val.(projectionResult); ok {
		if len(pr) == 1 {
			val = pr[0]
		} else {
			val = []interface{}(pr)
		}
	}
	result := applyFunction(fn, val, nil, nil)
	if result == nil {
		return nil, true, ErrJSONPathSyntax
	}
	if fn == "keys" {
		// keys() replies its array bare (probed: keys($.obj) -> ["b","a"],
		// not [["b","a"]]).
		return result, true, nil
	}
	return projectionResult{wrapProjectionResult(fn, result)}, true, nil
}

func wrapProjectionResult(fn string, result interface{}) interface{} {
	switch fn {
	case "length", "count":
		if f, ok := toFloat(result); ok {
			return int64(f)
		}
	case "sum", "min", "max", "avg", "stddev", "abs", "ceiling", "floor":
		if f, ok := toFloat(result); ok {
			return projectionFloat(f)
		}
	}
	return result
}

// walkParts resolves parts against current, returning the single value.
func walkParts(current interface{}, parts []string) (interface{}, error) {
	for _, part := range parts {
		if current == nil {
			return nil, fmt.Errorf("path not found")
		}
		if IsFilterPart(part) {
			// Only the first filter segment is handled by getPath.
			return nil, ErrJSONPathSyntax
		}

		switch c := current.(type) {
		case *OrderedObj:
			if part == "~" {
				keys := make([]interface{}, 0, c.Len())
				for _, k := range c.Keys() {
					keys = append(keys, k)
				}
				current = keys
				continue
			}
			val, ok := c.Get(part)
			if !ok {
				return nil, fmt.Errorf("ERR Path does not exist")
			}
			current = val
		case map[string]interface{}:
			if part == "~" {
				keys := make([]interface{}, 0, len(c))
				for k := range c {
					keys = append(keys, k)
				}
				sortStrings(keys)
				current = keys
				continue
			}
			val, ok := c[part]
			if !ok {
				return nil, fmt.Errorf("ERR Path does not exist")
			}
			current = val
		case []interface{}:
			index, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid array index: %s", part)
			}
			if index < 0 || index >= len(c) {
				return nil, fmt.Errorf("array index out of bounds")
			}
			current = c[index]
		default:
			return nil, fmt.Errorf("cannot traverse into %T", current)
		}
	}

	return current, nil
}

// setPath sets a value at the given path
func (jv *JSONValue) setPath(path string, value interface{}, nx, xx bool) (bool, error) {
	parts := parsePath(path)
	if len(parts) == 0 {
		jv.data = value
		return true, nil
	}

	// Navigate to parent
	parent := jv.data
	for i := 0; i < len(parts)-1; i++ {
		if parent == nil {
			return false, fmt.Errorf("path not found")
		}

		switch p := parent.(type) {
		case *OrderedObj:
			val, ok := p.Get(parts[i])
			if !ok {
				// Create intermediate objects
				obj := NewOrderedObj()
				p.Set(parts[i], obj)
				val = obj
			}
			parent = val
		case map[string]interface{}:
			val, ok := p[parts[i]]
			if !ok {
				// Create intermediate objects
				p[parts[i]] = interface{}(NewOrderedObj())
				val = p[parts[i]]
			}
			parent = val
		case []interface{}:
			index, err := strconv.Atoi(parts[i])
			if err != nil {
				return false, fmt.Errorf("invalid array index")
			}
			if index < 0 || index >= len(p) {
				return false, fmt.Errorf("array index out of bounds")
			}
			parent = p[index]
		default:
			return false, fmt.Errorf("cannot traverse into %T", parent)
		}
	}

	// Set the value
	lastPart := parts[len(parts)-1]
	switch p := parent.(type) {
	case *OrderedObj:
		_, exists := p.Get(lastPart)
		if xx && !exists {
			return false, nil
		}
		if nx && exists {
			return false, nil
		}
		p.Set(lastPart, value)
		return true, nil
	case map[string]interface{}:
		if xx {
			if _, ok := p[lastPart]; !ok {
				return false, nil
			}
		}
		if nx {
			if _, ok := p[lastPart]; ok {
				return false, nil
			}
		}
		p[lastPart] = value
		return true, nil
	case []interface{}:
		index, err := strconv.Atoi(lastPart)
		if err != nil {
			return false, fmt.Errorf("invalid array index")
		}
		if index < 0 || index >= len(p) {
			return false, fmt.Errorf("array index out of bounds")
		}
		p[index] = value
		return true, nil
	default:
		return false, fmt.Errorf("cannot set value in %T", parent)
	}
}

// delPath deletes a value at the given path
func (jv *JSONValue) delPath(path string) (bool, error) {
	parts := parsePath(path)
	if len(parts) == 0 {
		jv.data = nil
		return true, nil
	}
	// Recursive descent deletes every match (probed: JSON.DEL $..b removes
	// all descendant b's with swap-delete per container).
	if strings.HasPrefix(path, "$") && hasRecursiveDescent(normalizeJSONPath(path)) {
		// Callers (Del) already hold the lock — do not re-lock.
		n := jv.DelRecursive(path)
		return n > 0, nil
	}

	// Navigate to parent
	parent := jv.data
	for i := 0; i < len(parts)-1; i++ {
		if parent == nil {
			return false, nil
		}

		switch p := parent.(type) {
		case *OrderedObj:
			val, ok := p.Get(parts[i])
			if !ok {
				return false, nil
			}
			parent = val
		case map[string]interface{}:
			val, ok := p[parts[i]]
			if !ok {
				return false, nil
			}
			parent = val
		case []interface{}:
			index, err := strconv.Atoi(parts[i])
			if err != nil {
				return false, fmt.Errorf("invalid array index")
			}
			if index < 0 || index >= len(p) {
				return false, fmt.Errorf("array index out of bounds")
			}
			parent = p[index]
		default:
			return false, fmt.Errorf("cannot traverse into %T", parent)
		}
	}

	// Delete the value
	lastPart := parts[len(parts)-1]
	switch p := parent.(type) {
	case *OrderedObj:
		if p.Delete(parts[len(parts)-1]) {
			return true, nil
		}
		return false, nil
	case map[string]interface{}:
		if _, ok := p[lastPart]; ok {
			delete(p, lastPart)
			return true, nil
		}
		return false, nil
	case []interface{}:
		index, err := strconv.Atoi(lastPart)
		if err != nil {
			return false, fmt.Errorf("invalid array index")
		}
		if index < 0 || index >= len(p) {
			return false, fmt.Errorf("array index out of bounds")
		}
		// Remove element from slice
		p = append(p[:index], p[index+1:]...)
		return true, nil
	default:
		return false, fmt.Errorf("cannot delete from %T", parent)
	}
}

// parsePath parses a JSON path into components
func parsePath(path string) []string {
	// Remove leading $. or .
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")

	if path == "" {
		return nil
	}

	// Split by . for object keys
	// Handle array indices like .foo[0].bar; nested brackets (JSONPath
	// filter literals such as [?(@.a==[1,2])]) stay inside their segment.
	// A trailing `~` (property-name suffix, `$.obj~`) is split off into its
	// own part; the dotted form `$.obj.~` is rejected earlier as a syntax
	// error (real 8.10 grammar).
	var parts []string
	current := ""
	appendPart := func(p string) {
		if len(p) > 1 && strings.HasSuffix(p, "~") {
			parts = append(parts, strings.TrimSuffix(p, "~"), "~")
			return
		}
		parts = append(parts, p)
	}
	depth := 0

	for i, ch := range path {
		if ch == '[' {
			if depth == 0 && current != "" {
				appendPart(current)
				current = ""
			}
			depth++
			if depth > 1 {
				current += string(ch) // keep nested (filter literal) brackets
			}
		} else if ch == ']' {
			depth--
			if depth > 0 {
				current += string(ch)
			} else if current != "" {
				appendPart(current)
				current = ""
			}
		} else if ch == '.' && depth == 0 {
			if current != "" {
				appendPart(current)
				current = ""
			}
		} else {
			current += string(ch)
			// Handle last character
			if i == len(path)-1 && current != "" {
				appendPart(current)
			}
		}
	}

	return parts
}
