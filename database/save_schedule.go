package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/logger"
)

// savePoint is one Redis "save <seconds> <changes>" rule.
type savePoint struct {
	seconds int64
	changes int64
}

// parseSaveConfig parses CONFIG save value like "3600 1 300 100 60 10000".
// Empty / odd-length / invalid tokens yield no points (autosave disabled).
func parseSaveConfig(s string) []savePoint {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) < 2 || len(fields)%2 != 0 {
		return nil
	}
	out := make([]savePoint, 0, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		sec, err1 := strconv.ParseInt(fields[i], 10, 64)
		chg, err2 := strconv.ParseInt(fields[i+1], 10, 64)
		if err1 != nil || err2 != nil || sec < 0 || chg < 0 {
			return nil
		}
		if sec == 0 && chg == 0 {
			continue
		}
		out = append(out, savePoint{seconds: sec, changes: chg})
	}
	return out
}

func (server *Server) incrDirty() {
	if server == nil {
		return
	}
	server.dirty.Add(1)
}

func (server *Server) resetDirtyAfterSave() {
	if server == nil {
		return
	}
	server.dirty.Store(0)
	server.lastSaveUnix.Store(time.Now().Unix())
}

// DirtyChanges returns rdb_changes_since_last_save.
func (server *Server) DirtyChanges() int64 {
	if server == nil {
		return 0
	}
	return server.dirty.Load()
}

// checkSavePoints triggers BGSAVE when any save point is satisfied (PE-4).
func (server *Server) checkSavePoints() {
	server.checkSavePointsWith(config.Properties.Save)
}

// checkSavePointsWith evaluates save rules from cfg (e.g. "3600 1 300 100").
func (server *Server) checkSavePointsWith(cfg string) {
	if server == nil {
		return
	}
	points := parseSaveConfig(cfg)
	if len(points) == 0 {
		return
	}
	server.bgsaveMu.Lock()
	running := server.bgsaveRunning
	server.bgsaveMu.Unlock()
	if running {
		return
	}
	if server.masterStatus != nil {
		server.masterStatus.mu.RLock()
		replSaving := server.masterStatus.bgSaveState == bgSaveRunning
		server.masterStatus.mu.RUnlock()
		if replSaving {
			return
		}
	}

	dirty := server.dirty.Load()
	last := server.lastSaveUnix.Load()
	if last == 0 {
		last = time.Now().Unix()
	}
	elapsed := time.Now().Unix() - last
	for _, p := range points {
		if elapsed >= p.seconds && dirty >= p.changes {
			logger.Info("Automatic BGSAVE triggered by save point")
			_ = BGSaveRDB(server, nil)
			return
		}
	}
}
