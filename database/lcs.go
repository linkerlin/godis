package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execLCS returns the longest common subsequence of two strings
// LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min-match-len] [WITHMATCHLEN]
func execLCS(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'lcs' command")
	}

	key1 := string(args[0])
	key2 := string(args[1])

	// Parse options
	showLen := false
	showIdx := false
	minMatchLen := 0
	withMatchLen := false

	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "LEN":
			showLen = true
		case "IDX":
			showIdx = true
		case "MINMATCHLEN":
			if i+1 >= len(args) {
				return protocol.MakeErrReply("ERR syntax error")
			}
			val, err := strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			minMatchLen = val
			i++
		case "WITHMATCHLEN":
			withMatchLen = true
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	// Missing keys are treated as empty strings (Redis semantics).
	var str1, str2 []byte
	if entity1, exists := db.GetEntity(key1); exists {
		b, ok := entity1.Data.([]byte)
		if !ok {
			return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		str1 = b
	}
	if entity2, exists := db.GetEntity(key2); exists {
		b, ok := entity2.Data.([]byte)
		if !ok {
			return protocol.MakeErrReply("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		str2 = b
	}

	// Compute LCS
	lcsStr, matches := computeLCS(string(str1), string(str2), minMatchLen)

	// Return based on options
	if showLen && !showIdx {
		return protocol.MakeIntReply(int64(len(lcsStr)))
	}

	if showIdx {
		// RESP3 Map {matches, len}; RESP2 flat field array via Map.ToBytes.
		matchReplies := make([]redis.Reply, 0, len(matches))
		for _, match := range matches {
			if match.len < minMatchLen {
				continue
			}
			matchInfo := make([]redis.Reply, 0, 3)

			range1 := []redis.Reply{
				protocol.MakeIntReply(int64(match.start1)),
				protocol.MakeIntReply(int64(match.start1 + match.len - 1)),
			}
			matchInfo = append(matchInfo, protocol.MakeMultiRawReply(range1))

			range2 := []redis.Reply{
				protocol.MakeIntReply(int64(match.start2)),
				protocol.MakeIntReply(int64(match.start2 + match.len - 1)),
			}
			matchInfo = append(matchInfo, protocol.MakeMultiRawReply(range2))

			if withMatchLen {
				matchInfo = append(matchInfo, protocol.MakeIntReply(int64(match.len)))
			}

			matchReplies = append(matchReplies, protocol.MakeMultiRawReply(matchInfo))
		}
		m := protocol.MakeMapReply()
		m.Put("matches", protocol.MakeMultiRawReply(matchReplies))
		m.Put("len", protocol.MakeIntReply(int64(len(lcsStr))))
		return m
	}

	// Default: return the LCS string
	return protocol.MakeBulkReply([]byte(lcsStr))
}

// lcsMatch represents a matching substring
type lcsMatch struct {
	start1 int
	start2 int
	len    int
}

// computeLCS computes the longest common subsequence and matching positions
func computeLCS(s1, s2 string, minLen int) (string, []lcsMatch) {
	m, n := len(s1), len(s2)
	if m == 0 || n == 0 {
		return "", nil
	}

	// Dynamic programming table
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	// Fill DP table
	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if s1[i-1] == s2[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				if dp[i-1][j] > dp[i][j-1] {
					dp[i][j] = dp[i-1][j]
				} else {
					dp[i][j] = dp[i][j-1]
				}
			}
		}
	}

	// Reconstruct LCS
	lcs := make([]byte, 0, dp[m][n])
	i, j := m, n
	for i > 0 && j > 0 {
		if s1[i-1] == s2[j-1] {
			lcs = append(lcs, s1[i-1])
			i--
			j--
		} else if dp[i-1][j] > dp[i][j-1] {
			i--
		} else {
			j--
		}
	}

	// Reverse LCS
	for i, j := 0, len(lcs)-1; i < j; i, j = i+1, j-1 {
		lcs[i], lcs[j] = lcs[j], lcs[i]
	}

	// Find matching substrings (simplified)
	matches := findMatches(s1, s2, minLen)

	return string(lcs), matches
}

// findMatches finds matching substrings between two strings
func findMatches(s1, s2 string, minLen int) []lcsMatch {
	var matches []lcsMatch
	m, n := len(s1), len(s2)

	// Simple approach: find all matching substrings
	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			length := 0
			for i+length < m && j+length < n && s1[i+length] == s2[j+length] {
				length++
			}
			if length >= minLen {
				matches = append(matches, lcsMatch{
					start1: i,
					start2: j,
					len:    length,
				})
			}
		}
	}

	return matches
}

func init() {
	registerCommand("LCS", execLCS, prepareLCSKeys, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 2, 1)
}

func prepareLCSKeys(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	return nil, []string{string(args[0]), string(args[1])}
}
