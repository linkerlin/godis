package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execTime 处理 TIME 命令
// 返回当前服务器时间的 UNIX 时间戳（秒）和微秒
func execTime(args [][]byte) redis.Reply {
	now := time.Now()
	seconds := now.Unix()
	microseconds := int64(now.Nanosecond()) / 1000

	result := [][]byte{
		[]byte(strconv.FormatInt(seconds, 10)),
		[]byte(strconv.FormatInt(microseconds, 10)),
	}
	return protocol.MakeMultiBulkReply(result)
}

// execLolwut returns a Redis-compatible LOLWUT easter egg.
// LOLWUT [IT] [VERSION [ver]] — IT selects Italian poem mode; unsupported
// VERSION numbers print only a short version line (like Redis 8.10).
func execLolwut(args [][]byte) redis.Reply {
	italian := false
	verSet := false
	ver := 0
	for i := 0; i < len(args); {
		tok := strings.ToUpper(string(args[i]))
		switch tok {
		case "IT":
			italian = true
			i++
		case "VERSION":
			if i+1 >= len(args) {
				// Bare VERSION keeps the default visual/banner style.
				i++
				continue
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			ver = n
			verSet = true
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if italian {
		return protocol.MakeBulkReply([]byte(lolwutItalianPoem()))
	}

	// Redis only renders dedicated visuals for a few styles; others are version-only.
	if verSet && ver != 5 && ver != 6 {
		return protocol.MakeBulkReply([]byte("Godis ver. redis-compat"))
	}

	msg := "Godis ver. redis-compat"
	if verSet && ver > 0 {
		msg += " (LOLWUT style " + strconv.Itoa(ver) + ")"
	}
	msg += "\n" +
		"A Go Redis-compatible server.\n" +
		"For more information visit https://github.com/linkerlin/godis\n"
	return protocol.MakeBulkReply([]byte(msg))
}

func lolwutItalianPoem() string {
	// Static IT-mode stub (Redis generates verses via Balestrini's algorithm).
	return " I  CAPELLI      TRA LE LABBRA\n" +
		"  SI ESPANDE      RAPIDAMENTE\n" +
		"  LA TESTA   PREMUTA    SULLA SPALLA\n" +
		"  ASSUME     LA BEN NOTA FORMA    DI FUNGO\n" +
		"\n" +
		"In 1961, Nanni Balestrini created one of the first computer-generated poems, TAPE MARK I.\n" +
		"Use: LOLWUT IT for the original Italian output. Godis ver. redis-compat\n"
}

func init() {
	registerSpecialCommand("Time", 1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Role", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Lolwut", -1, 0).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}
