package json

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// JSONValue represents a JSON value stored in Redis
type JSONValue struct {
	data interface{}
	mu   sync.RWMutex
}

// NewJSONValue creates a new JSON value from a Go interface{}
func NewJSONValue(data interface{}) *JSONValue {
	return &JSONValue{data: data}
}

// NewJSONValueFromBytes creates a new JSON value from JSON bytes
func NewJSONValueFromBytes(data []byte) (*JSONValue, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &JSONValue{data: convertNumbers(v)}, nil
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
	case map[string]interface{}:
		return "object", nil
	case []interface{}:
		return "array", nil
	case string:
		return "string", nil
	case float64, float32, int, int64:
		return "number", nil
	case bool:
		return "boolean", nil
	default:
		return "unknown", nil
	}
}

// NumIncrBy increments a number at the given path by the specified amount
func (jv *JSONValue) NumIncrBy(path string, increment float64) (float64, error) {
	jv.mu.Lock()
	defer jv.mu.Unlock()
	
	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	
	var num float64
	switch v := val.(type) {
	case float64:
		num = v
	case float32:
		num = float64(v)
	case int:
		num = float64(v)
	case int64:
		num = float64(v)
	default:
		return 0, fmt.Errorf("value at path is not a number")
	}
	
	newVal := num + increment
	jv.setPath(path, newVal, false, false)
	return newVal, nil
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
		return 0, fmt.Errorf("value at path is not a string")
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
		return 0, fmt.Errorf("value at path is not an array")
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
		return 0, fmt.Errorf("value at path is not an array")
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
		return nil, fmt.Errorf("value at path is not an array")
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
		return 0, fmt.Errorf("value at path is not an array")
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
	
	obj, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("value at path is not an object")
	}
	
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	return keys, nil
}

// ObjLen returns the number of keys in an object at the given path
func (jv *JSONValue) ObjLen(path string) (int, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	
	val, err := jv.getPath(path)
	if err != nil {
		return 0, err
	}
	
	obj, ok := val.(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("value at path is not an object")
	}
	
	return len(obj), nil
}

// ToBytes returns the JSON value as bytes
func (jv *JSONValue) ToBytes() ([]byte, error) {
	jv.mu.RLock()
	defer jv.mu.RUnlock()
	
	return json.Marshal(jv.data)
}

// ToString returns the JSON value as a string
func (jv *JSONValue) ToString() (string, error) {
	b, err := jv.ToBytes()
	return string(b), err
}

// getPath traverses the JSON structure following the path
func (jv *JSONValue) getPath(path string) (interface{}, error) {
	parts := parsePath(path)
	current := jv.data
	
	for _, part := range parts {
		if current == nil {
			return nil, fmt.Errorf("path not found")
		}
		
		switch c := current.(type) {
		case map[string]interface{}:
			val, ok := c[part]
			if !ok {
				return nil, fmt.Errorf("key '%s' not found", part)
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
		case map[string]interface{}:
			val, ok := p[parts[i]]
			if !ok {
				// Create intermediate objects
				p[parts[i]] = make(map[string]interface{})
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
	
	// Navigate to parent
	parent := jv.data
	for i := 0; i < len(parts)-1; i++ {
		if parent == nil {
			return false, nil
		}
		
		switch p := parent.(type) {
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
	// Handle array indices like .foo[0].bar
	var parts []string
	current := ""
	inBracket := false
	
	for i, ch := range path {
		if ch == '[' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
			inBracket = true
		} else if ch == ']' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
			inBracket = false
		} else if ch == '.' && !inBracket {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(ch)
			// Handle last character
			if i == len(path)-1 && current != "" {
				parts = append(parts, current)
			}
		}
	}
	
	return parts
}
