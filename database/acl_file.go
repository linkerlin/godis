package database

import (
	"os"
	"path/filepath"

	"github.com/linkerlin/godis/config"
)

func resolveACLFilePath() string {
	if config.Properties == nil || config.Properties.AclFile == "" {
		return ""
	}
	p := config.Properties.AclFile
	if !filepath.IsAbs(p) {
		if config.Properties.Dir != "" {
			p = filepath.Join(config.Properties.Dir, p)
		}
	}
	return p
}

func aclFileConfigured() bool {
	return resolveACLFilePath() != ""
}

func aclFileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
