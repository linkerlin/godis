//go:build linux

package e6chaos

import (
	"os"
	"syscall"
)

// freeze/signal the partition drill (linux only; other platforms skip the test).
func sigstop(p *os.Process) error { return p.Signal(syscall.SIGSTOP) }
func sigcont(p *os.Process) error { return p.Signal(syscall.SIGCONT) }
