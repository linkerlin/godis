package database

// Redis 8.6–8.8 stream additions: XNACK (8.8), XADD IDMP/IDMPAUTO (8.6) and
// XCFGSET per-stream retention settings.

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/zeebo/xxh3"
)

// ensureStreamIdmpDefaults applies the global stream-idmp-* retention
// defaults to streams that have no per-stream XCFGSET override.
func ensureStreamIdmpDefaults(s *stream.Stream) {
	if s == nil || config.Properties == nil {
		return
	}
	if s.IdmpConfig() == (stream.StreamIdmpConfig{}) {
		s.SetIdmpConfig(stream.StreamIdmpConfig{
			Duration: time.Duration(config.Properties.StreamIdmpDuration) * time.Second,
			MaxSize:  config.Properties.StreamIdmpMaxSize,
		})
	}
}

// computeIdmpAutoID derives the IDMPAUTO idempotent ID from the entry
// content (sorted field/value pairs), so the same producer + same content
// deduplicates.
func computeIdmpAutoID(fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(fields[k])
		b.WriteByte(0)
	}
	return strconv.FormatUint(xxh3.HashString(b.String()), 16)
}

// execXNack XNACK key group <SILENT|FAIL|FATAL> IDS numids id [id ...]
// [RETRYCOUNT count] [FORCE]  (Redis 8.8)
// Releases pending entries back to the group as unowned (consumer="",
// delivery time 0) so any consumer can claim them immediately.
func execXNack(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xnack' command")
	}
	key := string(args[0])
	groupName := string(args[1])

	var mode string
	switch strings.ToUpper(string(args[2])) {
	case "SILENT", "FAIL", "FATAL":
		mode = strings.ToUpper(string(args[2]))
	default:
		return protocol.MakeErrReply("ERR Invalid XNACK mode. Must be SILENT, FAIL or FATAL")
	}
	if !strings.EqualFold(string(args[3]), "IDS") {
		return protocol.MakeSyntaxErrReply()
	}
	numIDs, err := strconv.Atoi(string(args[4]))
	if err != nil || numIDs < 0 {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	if 5+numIDs > len(args) {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xnack' command")
	}
	ids := make([]stream.StreamID, 0, numIDs)
	for i := 0; i < numIDs; i++ {
		id, perr := stream.ParseStreamID(string(args[5+i]), stream.StreamID{})
		if perr != nil {
			return protocol.MakeErrReply("ERR Invalid stream ID specified as stream command argument")
		}
		ids = append(ids, id)
	}

	retryCount := -1
	force := false
	for i := 5 + numIDs; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "RETRYCOUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			n, perr := strconv.Atoi(string(args[i+1]))
			if perr != nil || n < 0 {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			retryCount = n
			i++
		case "FORCE":
			force = true
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}
	group, gerr := s.GetGroup(groupName)
	if gerr != nil {
		return protocol.MakeErrReply("NOGROUP No such key '" + key + "' or consumer group '" + groupName + "'")
	}

	released := 0
	for _, id := range ids {
		pe, ok := group.Pending[id]
		if !ok {
			if !force || s.GetEntry(id) == nil {
				continue
			}
			pe = &stream.PendingEntry{ID: id}
			group.Pending[id] = pe
		} else if pe.Consumer != "" {
			if raw, exists := group.Consumers.Get(pe.Consumer); exists {
				delete(raw.(*stream.Consumer).Pending, id)
			}
		}

		switch {
		case retryCount >= 0:
			pe.DeliveryCount = retryCount
		case mode == "SILENT":
			if pe.DeliveryCount > 0 {
				pe.DeliveryCount--
			}
		case mode == "FATAL":
			pe.DeliveryCount = math.MaxInt64
		} // FAIL keeps the counter

		pe.Consumer = ""
		pe.DeliveryTime = time.Time{}
		released++
	}

	// PEL state is not AOF-replicated (consistent with XREADGROUP delivery).
	return protocol.MakeIntReply(int64(released))
}

// execXCfgSet XCFGSET key IDMP-DURATION seconds IDMP-MAXSIZE n (Redis 8.6)
// Either setting may be omitted; pairs may appear in any order.
func execXCfgSet(db *DB, args [][]byte) redis.Reply {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'xcfgset' command")
	}
	key := string(args[0])
	s, errReply := db.getAsStream(key)
	if errReply != nil {
		return errReply
	}
	if s == nil {
		return protocol.MakeErrReply("ERR no such key")
	}
	ensureStreamIdmpDefaults(s)
	cfg := s.IdmpConfig()
	for i := 1; i+1 < len(args); i += 2 {
		value := string(args[i+1])
		switch strings.ToUpper(string(args[i])) {
		case "IDMP-DURATION":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR IDMP-DURATION must be a non-negative integer")
			}
			cfg.Duration = time.Duration(n) * time.Second
		case "IDMP-MAXSIZE":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR IDMP-MAXSIZE must be a non-negative integer")
			}
			cfg.MaxSize = n
		default:
			return protocol.MakeErrReply("ERR Unsupported stream configuration argument: '" + string(args[i]) + "'")
		}
	}
	s.SetIdmpConfig(cfg)
	return protocol.MakeOkReply()
}

func init() {
	registerCommand("XNack", execXNack, writeFirstKey, nil, -7, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
	registerCommand("XCfgSet", execXCfgSet, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagFast}, 1, 1, 1)
}
