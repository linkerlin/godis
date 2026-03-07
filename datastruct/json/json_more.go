package json

import (
	"fmt"
)

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
