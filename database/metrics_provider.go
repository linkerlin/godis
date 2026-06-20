package database

var slowLogLenProvider func() int

// SetSlowLogLenProvider registers a callback for slowlog length metrics.
func SetSlowLogLenProvider(fn func() int) {
	slowLogLenProvider = fn
}

// SlowLogLength returns the current slowlog entry count.
func SlowLogLength() int {
	if slowLogLenProvider != nil {
		return slowLogLenProvider()
	}
	return 0
}
