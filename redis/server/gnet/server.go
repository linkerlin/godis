package gnet

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/database"
	idatabase "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/stats"
	gatomic "github.com/linkerlin/godis/lib/sync/atomic"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/tcp"
	"github.com/panjf2000/gnet/v2"
)

const shutdownWaitTimeout = 10 * time.Second

type GnetServer struct {
	gnet.BuiltinEventEngine
	eng      gnet.Engine
	booted   atomic.Bool
	db       idatabase.DB
	closing  gatomic.Boolean
	inFlight sync.WaitGroup
}

func NewGnetServer(db idatabase.DB) *GnetServer {
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
	if !tcp.TryAcceptClient() {
		return tcp.MaxClientsErrReply, gnet.Close
	}
	client := connection.NewConn(c)
	c.SetContext(client)
	database.RegisterClient(client)
	return
}

func (s *GnetServer) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	if err != nil {
		logger.Infof("error occurred on connection=%s, %v\n", c.RemoteAddr().String(), err)
	}
	if ctx := c.Context(); ctx != nil {
		tcp.ReleaseClient()
		conn := ctx.(redis.Connection)
		database.UnregisterClient(conn)
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
		// Reply with a protocol error before closing, matching Redis 8 (the
		// std server path already does this).
		logger.Infof("parse command line failed: %v", err)
		errMsg := strings.TrimPrefix(err.Error(), "protocol error: ")
		_, _ = c.Write(protocol.MakeErrReply("ERR Protocol error: " + errMsg).ToBytes())
		return gnet.Close
	}
	if len(cmdLine) == 0 {
		return gnet.None
	}
	result := s.db.Exec(conn, cmdLine)
	if result == nil {
		return gnet.None
	}
	cmdName := strings.ToLower(string(cmdLine[0]))
	suppress := false
	if cmdName != "client" {
		if sreply, ok := conn.(interface{ ShouldSuppressReply() bool }); ok {
			suppress = sreply.ShouldSuppressReply()
		}
	}
	if suppress {
		return gnet.None
	}
	var buffer []byte
	if conn.GetProtocolVersion() == 3 {
		buffer = protocol.ReplyToRESP3(result)
	} else {
		buffer = result.ToBytes()
	}
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
