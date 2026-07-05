package std

/*
 * A tcp.Handler implements redis protocol
 */

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/linkerlin/godis/cluster"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/database"
	idatabase "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/stats"
	gatomic "github.com/linkerlin/godis/lib/sync/atomic"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/tcp"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// Handler implements tcp.Handler and serves as a redis server
type Handler struct {
	activeConn sync.Map // *client -> placeholder
	db         idatabase.DB
	closing    gatomic.Boolean // refusing new client and new request
	inFlight   sync.WaitGroup  // tracks active Handle goroutines
}

// MakeHandler creates a Handler instance
func MakeHandler() (*Handler, error) {
	var db idatabase.DB
	var err error
	if config.Properties.ClusterEnable {
		db, err = cluster.MakeCluster()
		if err != nil {
			return nil, errors.Wrap(err, "create cluster failed")
		}
	} else {
		db, err = database.NewStandaloneServer()
		if err != nil {
			return nil, errors.Wrap(err, "create standalone server failed")
		}
	}
	return &Handler{
		db: db,
	}, nil
}

func Serve(addr string, handler *Handler) error {
	return tcp.ListenAndServeWithSignal(&tcp.Config{
		Address: addr,
	}, handler)
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}
	h.inFlight.Add(1)
	defer h.inFlight.Done()

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if h.closing.Get() {
			break
		}
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			// protocol err
			errReply := protocol.MakeErrReply(payload.Err.Error())
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			var resultBytes []byte
			if client.GetProtocolVersion() == 3 {
				resultBytes = protocol.ReplyToRESP3(result)
			} else {
				resultBytes = result.ToBytes()
			}
			_, _ = client.Write(resultBytes)
			stats.RecordOutput(len(resultBytes))
		} else {
			_, _ = client.Write(unknownErrReplyBytes)
			stats.RecordOutput(len(unknownErrReplyBytes))
		}
	}
}

// Close stops handler and waits for in-flight connections to finish.
func (h *Handler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)

	done := make(chan struct{})
	go func() {
		h.inFlight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		logger.Warn("graceful shutdown timed out, force closing connections")
	}

	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
