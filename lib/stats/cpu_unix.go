//go:build unix

package stats

import "syscall"

// GetProcessCPUTime returns cumulative user/system CPU seconds for this process.
func GetProcessCPUTime() (userSec, sysSec float64) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, 0
	}
	userSec = float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	sysSec = float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	return userSec, sysSec
}
