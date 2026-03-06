// Package pool provides object pools for performance optimization
package pool

import (
	"sync"
)

// BufferPool is a pool for byte slices
var BufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

// GetBuffer gets a buffer from pool
func GetBuffer() []byte {
	return BufferPool.Get().([]byte)
}

// PutBuffer returns a buffer to pool
func PutBuffer(buf []byte) {
	if cap(buf) <= 1024*1024 { // Only pool buffers <= 1MB
		BufferPool.Put(buf[:0])
	}
}

// StringPool is a pool for strings
var StringPool = sync.Pool{
	New: func() interface{} {
		return make([]string, 0, 16)
    },
}

// GetStringSlice gets a string slice from pool
func GetStringSlice() []string {
	return StringPool.Get().([]string)
}

// PutStringSlice returns a string slice to pool
func PutStringSlice(s []string) {
	if cap(s) <= 1024 {
		StringPool.Put(s[:0])
	}
}
