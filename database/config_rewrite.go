package database

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// rewriteConfigFile writes current CONFIG GET values back to the config file (CF-6).
// Returns nil on success, or an error reply.
func rewriteConfigFile() redis.Reply {
	path := config.GetConfigFilePath()
	if path == "" {
		return protocol.MakeErrReply("ERR The server is running without a config file")
	}

	desired := map[string]string{}
	for _, p := range getConfigMatches("*") {
		desired[p.key] = p.value
	}
	// Redis conf uses dbfilename; CONFIG GET exposes rdbfilename.
	if v, ok := desired["rdbfilename"]; ok {
		desired["dbfilename"] = v
		delete(desired, "rdbfilename")
	}
	if v, ok := desired["cluster-enabled"]; ok {
		desired["cluster-enable"] = v
		delete(desired, "cluster-enabled")
	}

	var existing []string
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			existing = append(existing, sc.Text())
		}
		_ = f.Close()
		if err := sc.Err(); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
	}

	written := make(map[string]bool)
	out := make([]string, 0, len(existing)+len(desired))
	for _, line := range existing {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") {
			out = append(out, line)
			continue
		}
		key, _, ok := splitConfigLineKey(trim)
		if !ok {
			out = append(out, line)
			continue
		}
		key = strings.ToLower(key)
		if val, ok := desired[key]; ok {
			out = append(out, formatConfigLine(key, val))
			written[key] = true
			continue
		}
		out = append(out, line)
	}
	for key, val := range desired {
		if written[key] {
			continue
		}
		out = append(out, formatConfigLine(key, val))
	}

	tmp := path + ".tmp"
	content := strings.Join(out, "\n")
	if len(out) > 0 && !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if err := os.WriteFile(tmp, []byte(content), 0644); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	if err := utils.ReplaceFile(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return protocol.MakeErrReply(fmt.Sprintf("ERR rewrite config: %v", err))
	}
	return nil
}

func splitConfigLineKey(line string) (key, value string, ok bool) {
	i := 0
	for i < len(line) && line[i] != ' ' && line[i] != '\t' {
		i++
	}
	if i == 0 {
		return "", "", false
	}
	key = line[:i]
	if i >= len(line) {
		return key, "", true
	}
	return key, strings.TrimSpace(line[i:]), true
}

func formatConfigLine(key, value string) string {
	if value == "" {
		return key + " \"\""
	}
	if strings.ContainsAny(value, " \t#\"") {
		escaped := strings.ReplaceAll(value, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		return key + ` "` + escaped + `"`
	}
	return key + " " + value
}
