//go:build windows

package database

import (
	"syscall"
	"unsafe"
)

type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

func getTotalSystemMemoryBytes() uint64 {
	var ms memoryStatusEx
	ms.Length = uint32(unsafe.Sizeof(ms))
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	proc := kernel32.NewProc("GlobalMemoryStatusEx")
	r, _, _ := proc.Call(uintptr(unsafe.Pointer(&ms)))
	if r == 0 {
		return 0
	}
	return ms.TotalPhys
}

// processMemoryCounters is a subset of Windows PROCESS_MEMORY_COUNTERS.
type processMemoryCounters struct {
	cb                         uint32
	pageFaultCount             uint32
	peakWorkingSetSize         uintptr
	workingSetSize             uintptr
	quotaPeakPagedPoolUsage    uintptr
	quotaPagedPoolUsage        uintptr
	quotaPeakNonPagedPoolUsage uintptr
	quotaNonPagedPoolUsage     uintptr
	pagefileUsage              uintptr
	peakPagefileUsage          uintptr
}

// getProcessRSSBytes returns the process working set (OS RSS), or 0 on failure.
func getProcessRSSBytes() uint64 {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	psapi := syscall.NewLazyDLL("psapi.dll")
	getCurrentProcess := kernel32.NewProc("GetCurrentProcess")
	getProcessMemoryInfo := psapi.NewProc("GetProcessMemoryInfo")
	handle, _, _ := getCurrentProcess.Call()
	var pmc processMemoryCounters
	pmc.cb = uint32(unsafe.Sizeof(pmc))
	r, _, _ := getProcessMemoryInfo.Call(handle, uintptr(unsafe.Pointer(&pmc)), uintptr(pmc.cb))
	if r == 0 {
		return 0
	}
	return uint64(pmc.workingSetSize)
}
