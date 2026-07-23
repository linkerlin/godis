package aof

import (
	"errors"
	"sync"

	"github.com/linkerlin/godis/lib/logger"
)

// ErrRewriteInProgress is returned when a background rewrite/RDB job is already running.
var ErrRewriteInProgress = errors.New("Background rewrite already in progress")

// rewriteJob serializes AOF rewrite and RDB generation that share temp files / AOF pause logic.
type rewriteJob struct {
	mu sync.Mutex
}

func (p *Persister) acquireRewriteSlot() error {
	if !p.rewriteJob.mu.TryLock() {
		return ErrRewriteInProgress
	}
	return nil
}

func (p *Persister) releaseRewriteSlot() {
	p.rewriteJob.mu.Unlock()
}

// RunRewriteAsync starts an AOF rewrite in the background.
func (p *Persister) RunRewriteAsync() error {
	if err := p.acquireRewriteSlot(); err != nil {
		return err
	}
	go func() {
		defer p.releaseRewriteSlot()
		if err := p.rewriteLocked(); err != nil {
			logger.Errorf("background AOF rewrite failed: %v", err)
		}
	}()
	return nil
}

// RunGenerateRDBAsync generates an RDB snapshot in the background.
// onDone is called with the error result (nil on success) after the job finishes.
func (p *Persister) RunGenerateRDBAsync(rdbFilename string, onDone func(error)) error {
	if err := p.acquireRewriteSlot(); err != nil {
		return err
	}
	go func() {
		defer p.releaseRewriteSlot()
		err := p.generateRDBToFile(rdbFilename)
		if err != nil {
			logger.Errorf("background RDB save failed: %v", err)
		}
		if onDone != nil {
			onDone(err)
		}
	}()
	return nil
}
