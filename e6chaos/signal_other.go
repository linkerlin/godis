//go:build !linux

package e6chaos

import "os"

// Stubs: the SIGSTOP partition drill skips on non-linux platforms, so these
// are never called — they only keep the package compiling.
func sigstop(*os.Process) error { return nil }
func sigcont(*os.Process) error { return nil }
