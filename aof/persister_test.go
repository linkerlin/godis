package aof

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	idatabase "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

type fakeDBEngine struct {
	mu    sync.Mutex
	execs [][][]byte
}

func (f *fakeDBEngine) Exec(_ redis.Connection, cmdLine [][]byte) redis.Reply {
	f.mu.Lock()
	f.execs = append(f.execs, cmdLine)
	f.mu.Unlock()
	return protocol.MakeOkReply()
}

func (f *fakeDBEngine) execsCopy() [][][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][][]byte, len(f.execs))
	copy(out, f.execs)
	return out
}

func (f *fakeDBEngine) AfterClientClose(redis.Connection) {}
func (f *fakeDBEngine) Close()                            {}
func (f *fakeDBEngine) LoadRDB(*core.Decoder) error       { return errors.New("no rdb preamble") }
func (f *fakeDBEngine) ExecWithLock(c redis.Connection, cmdLine [][]byte) redis.Reply {
	return f.Exec(c, cmdLine)
}
func (f *fakeDBEngine) ExecMulti(redis.Connection, map[string]uint64, []idatabase.CmdLine) redis.Reply {
	return protocol.MakeOkReply()
}
func (f *fakeDBEngine) GetUndoLogs(int, [][]byte) ([]idatabase.CmdLine, error) { return nil, nil }
func (f *fakeDBEngine) ForEach(int, func(string, *idatabase.DataEntity, *time.Time) bool) error {
	return nil
}
func (f *fakeDBEngine) RWLocks(int, []string, []string) error { return nil }
func (f *fakeDBEngine) RWUnLocks(int, []string, []string) error {
	return nil
}
func (f *fakeDBEngine) GetDBSize(int) (int, int, error) { return 0, 0, nil }
func (f *fakeDBEngine) GetEntity(int, string) (*idatabase.DataEntity, bool, error) {
	return nil, false, nil
}
func (f *fakeDBEngine) GetExpiration(int, string) (*time.Time, error)     { return nil, nil }
func (f *fakeDBEngine) SetKeyInsertedCallback(idatabase.KeyEventCallback) {}
func (f *fakeDBEngine) SetKeyDeletedCallback(idatabase.KeyEventCallback)  {}

type recordingListener struct {
	mu   sync.Mutex
	cmds []idatabase.CmdLine
}

func (l *recordingListener) Callback(cmds []idatabase.CmdLine) {
	l.mu.Lock()
	l.cmds = append(l.cmds, cmds...)
	l.mu.Unlock()
}

func TestPersisterSaveCloseIdempotentAndListener(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "appendonly.aof")
	db := &fakeDBEngine{}
	p, err := NewPersister(db, filename, false, FsyncNo, nil)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	listener := &recordingListener{}
	p.listeners[listener] = struct{}{}
	p.SaveCmdLine(0, utils.ToCmdLine("SET", "k", "v"))
	p.Close()
	p.Close() // idempotent

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("read aof: %v", err)
	}
	body := string(data)
	for _, want := range []string{"SET", "k", "v"} {
		if !strings.Contains(body, want) {
			t.Fatalf("aof missing %q: %q", want, body)
		}
	}
	listener.mu.Lock()
	got := len(listener.cmds)
	listener.mu.Unlock()
	if got == 0 {
		t.Fatal("listener did not receive the AOF payload")
	}
	// Saving after Close must be a no-op, not a panic.
	p.SaveCmdLine(0, utils.ToCmdLine("SET", "late", "v"))
}

func TestLoadAofReplaysCommands(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "load.aof")
	selectCmd := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", "0")).ToBytes()
	setCmd := protocol.MakeMultiBulkReply(utils.ToCmdLine("SET", "k", "v")).ToBytes()
	if err := os.WriteFile(filename, append(selectCmd, setCmd...), 0600); err != nil {
		t.Fatalf("write aof: %v", err)
	}
	db := &fakeDBEngine{}
	p, err := NewPersister(db, filename, true, FsyncNo, nil)
	if err != nil {
		t.Fatalf("NewPersister(load): %v", err)
	}
	p.Close()

	execs := db.execsCopy()
	found := false
	for _, cmd := range execs {
		if len(cmd) >= 3 && strings.EqualFold(string(cmd[0]), "SET") &&
			string(cmd[1]) == "k" && string(cmd[2]) == "v" {
			found = true
		}
	}
	if !found {
		t.Fatalf("AOF replay did not execute SET k v: %q", execs)
	}
}

func TestFsyncAlwaysWritesSynchronously(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "always.aof")
	db := &fakeDBEngine{}
	p, err := NewPersister(db, filename, false, FsyncAlways, nil)
	if err != nil {
		t.Fatalf("NewPersister: %v", err)
	}
	p.SaveCmdLine(0, utils.ToCmdLine("SET", "sync", "1"))
	// FsyncAlways writes on the caller goroutine before returning.
	data, err := os.ReadFile(filename)
	if err != nil || !strings.Contains(string(data), "sync") {
		t.Fatalf("fsync=always did not write synchronously: %q/%v", data, err)
	}
	p.Close()
}
