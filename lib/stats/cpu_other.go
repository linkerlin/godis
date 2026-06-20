//go:build !unix

package stats

// GetProcessCPUTime returns cumulative CPU seconds; unsupported on this platform.
func GetProcessCPUTime() (userSec, sysSec float64) {
	return 0, 0
}
