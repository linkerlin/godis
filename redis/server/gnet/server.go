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
	// flightMu serializes the closing flag against inFlight.Add so Wait never
	// races an Add-from-zero (sync.WaitGroup forbids exactly that pair; the
	// nightly -race gate flagged Close's waiter vs OnTraffic's Add).
	flightMu sync.Mutex
}

func NewGnetServer(db idatabase.DB) *GnetServer {
	s := &GnetServer{
		db: db,
	}
	database.SetShutdownHook(func() {
		_ = s.Close()
	})
	return s
}

func isMayBlockCmd(cmdLine [][]byte) bool {
	if len(cmdLine) == 0 {
		return false
	}
	switch strings.ToLower(string(cmdLine[0])) {
	case "blpop", "brpop", "brpoplpush", "blmove", "blmovem", "blmpop",
		"bzpopmin", "bzpopmax", "bzmpop", "wait", "waitaof", "failover":
		return true
	case "xread", "xreadgroup", "ts.read":
		for i := 1; i < len(cmdLine); i++ {
			if strings.EqualFold(string(cmdLine[i]), "block") {
				return true
			}
		}
	case "debug":
		if len(cmdLine) >= 2 && strings.EqualFold(string(cmdLine[1]), "sleep") {
			return true
		}
	}
	return false
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
	s.flightMu.Lock()
	if s.closing.Get() {
		s.flightMu.Unlock()
		return gnet.Close
	}
	s.inFlight.Add(1)
	s.flightMu.Unlock()
	defer s.inFlight.Done()

	conn := c.Context().(redis.Connection)
	var outBuf []byte

	for {
		buf, _ := c.Peek(-1)
		if len(buf) == 0 {
			break
		}
		cmdLine, consumed, complete, err := parser.ParseFrame(buf)
		if err != nil {
			logger.Infof("parse command line failed: %v", err)
			errMsg := strings.TrimPrefix(err.Error(), "protocol error: ")
			outBuf = append(outBuf, protocol.MakeErrReply("ERR Protocol error: "+errMsg).ToBytes()...)
			_, _ = c.Write(outBuf)
			return gnet.Close
		}
		if !complete {
			// Incomplete frame (TCP half-packet): leave bytes in gnet buffer for next OnTraffic.
			break
		}
		_, _ = c.Discard(consumed)
		if len(cmdLine) == 0 {
			continue
		}
		if isMayBlockCmd(cmdLine) {
			if len(outBuf) > 0 {
				if _, err := c.Write(outBuf); err != nil {
					logger.Infof("write response failed: %v", err)
					return gnet.Close
				}
				stats.RecordOutput(len(outBuf))
				outBuf = outBuf[:0]
			}
			s.inFlight.Add(1)
			go func(cmd [][]byte) {
				defer s.inFlight.Done()
				result := s.db.Exec(conn, cmd)
				if result == nil {
					return
				}
				cmdName := strings.ToLower(string(cmd[0]))
				suppress := false
				if cmdName != "client" {
					if sreply, ok := conn.(interface{ ShouldSuppressReply() bool }); ok {
						suppress = sreply.ShouldSuppressReply()
					}
				}
				if suppress {
					return
				}
				var resp []byte
				if conn.GetProtocolVersion() == 3 {
					resp = protocol.ReplyToRESP3(result)
				} else {
					resp = result.ToBytes()
				}
				if len(resp) > 0 {
					_ = c.AsyncWrite(resp, nil)
					stats.RecordOutput(len(resp))
				}
			}(cmdLine)
			continue
		}

		result := s.db.Exec(conn, cmdLine)
		if result == nil {
			continue
		}
		cmdName := strings.ToLower(string(cmdLine[0]))
		suppress := false
		if cmdName != "client" {
			if sreply, ok := conn.(interface{ ShouldSuppressReply() bool }); ok {
				suppress = sreply.ShouldSuppressReply()
			}
		}
		if suppress {
			continue
		}
		var buffer []byte
		if conn.GetProtocolVersion() == 3 {
			buffer = protocol.ReplyToRESP3(result)
		} else {
			buffer = result.ToBytes()
		}
		outBuf = append(outBuf, buffer...)
	}

	if len(outBuf) > 0 {
		if _, err := c.Write(outBuf); err != nil {
			logger.Infof("write response failed: %v", err)
			return gnet.Close
		}
		stats.RecordOutput(len(outBuf))
	}
	return gnet.None
}

// Close stops the gnet engine after in-flight command handlers finish.
func (s *GnetServer) Close() error {
	logger.Info("gnet server shutting down...")
	s.flightMu.Lock()
	s.closing.Set(true)
	s.flightMu.Unlock()

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
