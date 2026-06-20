package gnet

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/stats"
	gatomic "github.com/linkerlin/godis/lib/sync/atomic"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/panjf2000/gnet/v2"
)

const shutdownWaitTimeout = 10 * time.Second

type GnetServer struct {
	gnet.BuiltinEventEngine
	eng       gnet.Engine
	booted    atomic.Bool
	connected int32
	db        database.DB
	closing   gatomic.Boolean
	inFlight  sync.WaitGroup
}

func NewGnetServer(db database.DB) *GnetServer {
	return &GnetServer{
		db: db,
	}
}

func (s *GnetServer) Run(listenAddr string) error {
	return gnet.Run(s, "tcp://"+listenAddr, gnet.WithMulticore(true))
}

func (s *GnetServer) OnBoot(eng gnet.Engine) (action gnet.Action) {
	s.eng = eng
	s.booted.Store(true)
	return
}

func (s *GnetServer) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	if s.closing.Get() {
		return nil, gnet.Close
	}
	client := connection.NewConn(c)
	c.SetContext(client)
	atomic.AddInt32(&s.connected, 1)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	atomic.AddInt32(&s.connected, -1)
	if ctx := c.Context(); ctx != nil {
		conn := ctx.(redis.Connection)
		s.db.AfterClientClose(conn)
	}
	return
}

func (s *GnetServer) OnTraffic(c gnet.Conn) (action gnet.Action) {
	if s.closing.Get() {
		return gnet.Close
	}
	s.inFlight.Add(1)
	defer s.inFlight.Done()

	conn := c.Context().(redis.Connection)
	cmdLine, err := parser.ParseV2(c)
	if err != nil {
		logger.Infof("parse command line failed: %v", err)
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	result := s.db.Exec(conn, cmdLine)
	if result == nil {
		return gnet.None
	}
	buffer := result.ToBytes()
	if len(buffer) > 0 {
		if _, err := c.Write(buffer); err != nil {
			logger.Infof("write response failed: %v", err)
			return gnet.Close
		}
		stats.RecordOutput(len(buffer))
	}
	return gnet.None
}

// Close stops the gnet engine after in-flight command handlers finish.
func (s *GnetServer) Close() error {
	logger.Info("gnet server shutting down...")
	s.closing.Set(true)

	done := make(chan struct{})
	go func() {
		s.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownWaitTimeout):
		logger.Warn("gnet graceful shutdown timed out, force stopping engine")
	}

	if s.booted.Load() {
		s.eng.Stop(context.Background())
	}
	s.db.Close()
	return nil
}
