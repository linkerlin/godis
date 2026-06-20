// Package consts holds shared numeric limits consumed by both the runtime
// validator and (potentially) higher-level APIs.
//
// Conventions:
//   - Sizes are expressed in **bytes**.
//   - Values are immutable; do not expose mutable globals here.
package consts

// Hard limits enforced by lib/validate before a command reaches the engine.
const (
	MaxKeySize   = 512 * 1024        // 512 KiB
	MaxValueSize = 512 * 1024 * 1024 // 512 MiB
	MaxArgCount  = 1_000_000         // upper bound for arguments in a single command
)
