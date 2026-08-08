package utils

import (
	"io"
	"os"
)

// ReplaceFile renames src onto dst, replacing dst if it already exists.
//
// On Unix, os.Rename atomically replaces an existing destination. On Windows,
// Rename fails when dst exists ("Access is denied" / ERROR_ALREADY_EXISTS),
// so we remove dst first and retry. As a last resort (e.g. cross-device),
// copy then remove src.
func ReplaceFile(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	// Windows cannot rename over an existing file; remove then retry.
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		// Continue — rename may still work if remove failed for another reason.
	}
	if err := os.Rename(src, dst); err == nil {
		return nil
	} else {
		// Cross-device or still locked: copy contents then drop src.
		if copyErr := copyFileContents(src, dst); copyErr != nil {
			return err
		}
		_ = os.Remove(src)
		return nil
	}
}

func copyFileContents(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
