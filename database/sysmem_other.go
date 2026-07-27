//go:build !windows && !linux && !darwin && !freebsd && !openbsd

package database

func getTotalSystemMemoryBytes() uint64 {
	return 0
}
