package database

import (
	"crypto/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/acl"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/redis/protocol"
)

var aclEngine *acl.Engine

// ACLLogEntry 表示ACL日志条目
type ACLLogEntry struct {
	Count      int64
	Reason     string
	Context    string
	Object     string
	Username   string
	AgeSeconds float64
	Timestamp  time.Time // last occurrence
	EntryID    uint64    // monotonic entry id (Redis 8 ACL LOG)
	CreatedAt  time.Time // first occurrence
	ClientInfo string    // client line at last occurrence (Redis 8)
}

// ACL日志存储
var (
	aclLogEntries []*ACLLogEntry
	aclLogMu      sync.RWMutex
	aclLogNextID  uint64
)

func getACLLogMaxLen() int {
	if config.Properties != nil && config.Properties.AclLogMaxLen > 0 {
		return config.Properties.AclLogMaxLen
	}
	return 128
}

func trimACLLogToMax(maxLen int) {
	if maxLen <= 0 {
		maxLen = 128
	}
	aclLogMu.Lock()
	defer aclLogMu.Unlock()
	if len(aclLogEntries) > maxLen {
		aclLogEntries = aclLogEntries[len(aclLogEntries)-maxLen:]
	}
}

// addACLLogEntry 添加ACL日志条目
func addACLLogEntry(c redis.Connection, reason, context, object, username string) {
	aclLogMu.Lock()
	defer aclLogMu.Unlock()

	now := time.Now()
	clientInfo := ""
	if c != nil {
		clientInfo = formatClientListLine(c)
	}

	// 检查是否存在相同条目（去重）
	for _, entry := range aclLogEntries {
		if entry.Reason == reason && entry.Object == object && entry.Username == username {
			entry.Count++
			entry.Timestamp = now
			entry.ClientInfo = clientInfo
			return
		}
	}

	// 添加新条目
	aclLogNextID++
	entry := &ACLLogEntry{
		Count:      1,
		Reason:     reason,
		Context:    context,
		Object:     object,
		Username:   username,
		Timestamp:  now,
		EntryID:    aclLogNextID,
		CreatedAt:  now,
		ClientInfo: clientInfo,
	}

	aclLogEntries = append(aclLogEntries, entry)

	// 限制日志数量
	maxLen := getACLLogMaxLen()
	if len(aclLogEntries) > maxLen {
		aclLogEntries = aclLogEntries[len(aclLogEntries)-maxLen:]
	}
}

// getACLLogEntries 获取ACL日志条目。
// Outer array; each entry is a Map (RESP3 %) / flat field array (RESP2).
func getACLLogEntries(count int) redis.Reply {
	aclLogMu.RLock()
	defer aclLogMu.RUnlock()

	entries := aclLogEntries
	if count > 0 && count < len(entries) {
		entries = entries[len(entries)-count:]
	}
	if len(entries) == 0 {
		return protocol.MakeMultiRawReply([]redis.Reply{})
	}

	replies := make([]redis.Reply, 0, len(entries))
	now := time.Now()
	// Most recent first (Redis order).
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		age := now.Sub(entry.Timestamp).Seconds()
		m := protocol.MakeMapReply()
		m.Put("count", protocol.MakeIntReply(entry.Count))
		m.Put("reason", protocol.MakeBulkReply([]byte(entry.Reason)))
		m.Put("context", protocol.MakeBulkReply([]byte(entry.Context)))
		m.Put("object", protocol.MakeBulkReply([]byte(entry.Object)))
		m.Put("username", protocol.MakeBulkReply([]byte(entry.Username)))
		m.Put("age-seconds", protocol.MakeDoubleReply(age))
		m.Put("client-info", protocol.MakeBulkReply([]byte(entry.ClientInfo)))
		m.Put("entry-id", protocol.MakeIntReply(int64(entry.EntryID)))
		createdMs := entry.CreatedAt.UnixMilli()
		updatedMs := entry.Timestamp.UnixMilli()
		m.Put("timestamp-created", protocol.MakeIntReply(createdMs))
		m.Put("timestamp-last-updated", protocol.MakeIntReply(updatedMs))
		m.Put("timestamp", protocol.MakeIntReply(updatedMs/1000))
		replies = append(replies, m)
	}
	return protocol.MakeMultiRawReply(replies)
}

// resetACLLog 重置ACL日志
func resetACLLog() {
	aclLogMu.Lock()
	defer aclLogMu.Unlock()

	aclLogEntries = make([]*ACLLogEntry, 0)
}

// InitACLEngine initializes the ACL engine
func (server *Server) InitACLEngine() {
	aclEngine = acl.NewEngine()

	aclPath := resolveACLFilePath()
	if aclFileExists(aclPath) {
		if err := aclEngine.LoadFromFile(aclPath); err != nil {
			logger.Errorf("load aclfile %s failed: %+v, falling back to default user", aclPath, err)
		} else {
			return
		}
	}

	rules := []string{"on", "+@all", "~*", "&*"}
	if config.Properties.RequirePass != "" {
		rules = append(rules, ">"+config.Properties.RequirePass)
	} else {
		rules = append(rules, "nopass")
	}
	_, _ = aclEngine.SetUser("default", rules)
}

// execACL handles ACL subcommands
func execACL(db *DB, args [][]byte) redis.Reply {
	return execACLConn(nil, db, args)
}

// execACLConn handles ACL with optional connection context (for WHOAMI)
func execACLConn(c redis.Connection, db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "WHOAMI":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|whoami' command")
		}
		return formatACLWhoami(c)
	case "LIST":
		return execACLList(args[1:])
	case "USERS":
		return execACLUsers(args[1:])
	case "GETUSER":
		return execACLGetUser(args[1:])
	case "SETUSER":
		return execACLSetUser(args[1:])
	case "DELUSER":
		return execACLDelUser(args[1:])
	case "CAT":
		return execACLCat(args[1:])
	case "LOG":
		return execACLLog(args[1:])
	case "HELP":
		return execACLHelp(args[1:])
	case "GENPASS":
		return execACLGenPass(args[1:])
	case "DRYRUN":
		return execACLDryRun(db, args[1:])
	case "SAVE":
		return execACLSave(args[1:])
	case "LOAD":
		return execACLLoad(args[1:])
	default:
		return protocol.MakeErrReply("ERR Unknown ACL subcommand or wrong number of arguments for '" + subCmd + "'. Try ACL HELP.")
	}
}

// execACLWhoami returns current username
func execACLWhoami(args [][]byte) redis.Reply {
	return formatACLWhoami(nil)
}

// execACLList lists all users
func execACLList(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|list' command")
	}

	if aclEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	users := aclEngine.GetAllUsers()
	result := make([][]byte, len(users))
	for i, user := range users {
		if u, ok := aclEngine.GetUser(user); ok {
			result[i] = formatACLUser(u)
		}
	}

	return protocol.MakeMultiBulkReply(result)
}

// execACLUsers returns list of usernames
func execACLUsers(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|users' command")
	}

	if aclEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	users := aclEngine.GetAllUsers()
	result := make([][]byte, len(users))
	for i, user := range users {
		result[i] = []byte(user)
	}

	return protocol.MakeMultiBulkReply(result)
}

// execACLGetUser returns user details
func execACLGetUser(args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|getuser' command")
	}

	if aclEngine == nil {
		return &protocol.NullBulkReply{}
	}

	username := string(args[0])
	user, exists := aclEngine.GetUser(username)
	if !exists {
		return &protocol.NullBulkReply{}
	}

	return formatACLUserReply(user)
}

// execACLSetUser creates or modifies a user
func execACLSetUser(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|setuser' command")
	}

	if aclEngine == nil {
		return protocol.MakeErrReply("ERR ACL engine not initialized")
	}

	username := string(args[0])
	rules := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		rules[i-1] = string(args[i])
	}

	_, err := aclEngine.SetUser(username, rules)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}

	return protocol.MakeOkReply()
}

// execACLDelUser deletes users
func execACLDelUser(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|deluser' command")
	}

	if aclEngine == nil {
		return protocol.MakeIntReply(0)
	}

	names := make([]string, len(args))
	for i, arg := range args {
		names[i] = string(arg)
	}

	deleted := aclEngine.DelUser(names)
	return protocol.MakeIntReply(int64(deleted))
}

// execACLCat lists command categories
func execACLCat(args [][]byte) redis.Reply {
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.")
	}
	if aclEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if len(args) == 0 {
		// Redis 8.10 lists categories without '@'; Godis keeps extension cats (vector).
		categories := []string{
			"keyspace", "read", "write", "set", "sortedset", "list", "hash",
			"string", "array", "bitmap", "hyperloglog", "geo", "stream", "pubsub",
			"admin", "fast", "slow", "blocking", "dangerous", "connection",
			"transaction", "scripting",
			"bloom", "cuckoo", "cms", "topk", "tdigest", "search", "timeseries", "json",
			"vector",
		}
		result := make([][]byte, len(categories))
		for i, cat := range categories {
			result[i] = []byte(cat)
		}
		return protocol.MakeMultiBulkReply(result)
	}

	// List commands in a category. Redis 8.10 rejects an explicit '@' prefix here
	// (Unknown category '@read'); SETUSER rules still use @read.
	raw := string(args[0])
	if strings.HasPrefix(raw, "@") {
		return protocol.MakeErrReply("ERR Unknown category '" + raw + "'")
	}
	category := "@" + strings.ToLower(raw)
	cmds := commandsForACLCategory(category)
	if cmds == nil {
		return protocol.MakeErrReply("ERR Unknown category '" + raw + "'")
	}

	result := make([][]byte, len(cmds))
	for i, cmd := range cmds {
		result[i] = []byte(cmd)
	}
	return protocol.MakeMultiBulkReply(result)
}

// commandsForACLCategory returns commands for an @category (static map or derived from cmd flags).
func commandsForACLCategory(category string) []string {
	if cmds := acl.CommandCategoryMap[category]; cmds != nil {
		out := make([]string, len(cmds))
		copy(out, cmds)
		return out
	}
	switch category {
	case "@all":
		out := make([]string, 0, len(cmdTable))
		for name := range cmdTable {
			out = append(out, name)
		}
		sort.Strings(out)
		return out
	case "@fast":
		return listCmdsBySign(redisFlagFast)
	case "@blocking":
		return listCmdsBySign(redisFlagBlocking)
	case "@slow":
		fast := map[string]bool{}
		for _, n := range listCmdsBySign(redisFlagFast) {
			fast[n] = true
		}
		out := make([]string, 0, len(cmdTable))
		for name := range cmdTable {
			if !fast[name] {
				out = append(out, name)
			}
		}
		sort.Strings(out)
		return out
	case "@json":
		return listCmdsByPrefix("json.")
	case "@search":
		return listCmdsByPrefix("ft.")
	case "@vector":
		return listVectorCmds()
	case "@bloom":
		return listCmdsByPrefix("bf.")
	case "@cuckoo":
		return listCmdsByPrefix("cf.")
	case "@timeseries":
		return listCmdsByPrefix("ts.")
	case "@cms":
		return listCmdsByPrefix("cms.")
	case "@topk":
		return listCmdsByPrefix("topk.")
	case "@tdigest":
		return listCmdsByPrefix("tdigest.")
	default:
		return nil
	}
}

func listCmdsByPrefix(prefix string) []string {
	out := make([]string, 0)
	for name := range cmdTable {
		if strings.HasPrefix(name, prefix) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func listVectorCmds() []string {
	out := make([]string, 0)
	for name := range cmdTable {
		if strings.HasPrefix(name, "vs") {
			out = append(out, name)
			continue
		}
		switch name {
		case "vadd", "vsim", "vrem", "vcard", "vdim", "vemb", "vinfo",
			"vismember", "vrandmember", "vsetattr", "vgetattr", "vlinks", "vrange":
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func listCmdsBySign(sign string) []string {
	out := make([]string, 0)
	for name, cmd := range cmdTable {
		if cmd.extra == nil {
			continue
		}
		for _, s := range cmd.extra.signs {
			if s == sign {
				out = append(out, name)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// execACLLog manages the ACL log
func execACLLog(args [][]byte) redis.Reply {
	if len(args) == 0 {
		return getACLLogEntries(-1)
	}

	if strings.ToUpper(string(args[0])) == "RESET" {
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'LOG'. Try ACL HELP.")
		}
		resetACLLog()
		return protocol.MakeOkReply()
	}

	if len(args) != 1 {
		return protocol.MakeErrReply("ERR unknown subcommand or wrong number of arguments for 'LOG'. Try ACL HELP.")
	}
	c, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR value is not an integer or out of range")
	}
	count := -1
	if c > 0 {
		count = c
	}
	return getACLLogEntries(count)
}

// execACLHelp matches Redis ACL HELP layout.
func execACLHelp(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|help' command")
	}
	help := []string{
		"ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CAT [<category>]",
		"    List all commands that belong to <category>, or all command categories",
		"    when no category is specified.",
		"DELUSER <username> [<username> ...]",
		"    Delete a list of users.",
		"DRYRUN <username> <command> [<arg> ...]",
		"    Returns whether the user can execute the given command without executing the command.",
		"GETUSER <username>",
		"    Get the user's details.",
		"GENPASS [<bits>]",
		"    Generate a secure 256-bit user password. The optional `bits` argument can",
		"    be used to specify a different size.",
		"LIST",
		"    Show users details in config file format.",
		"LOAD",
		"    Reload users from the ACL file.",
		"LOG [<count> | RESET]",
		"    Show the ACL log entries.",
		"SAVE",
		"    Save the current config to the ACL file.",
		"SETUSER <username> <attribute> [<attribute> ...]",
		"    Create or modify a user with the specified attributes.",
		"USERS",
		"    List all the registered usernames.",
		"WHOAMI",
		"    Return the current connection username.",
		"HELP",
		"    Print this help.",
	}

	result := make([][]byte, len(help))
	for i, line := range help {
		result[i] = []byte(line)
	}
	return protocol.MakeMultiBulkReply(result)
}

// execACLGenPass generates a secure password
// ACL GENPASS [bits] — bits defaults to 256, must be multiple of 4 between 1 and 4096
func execACLGenPass(args [][]byte) redis.Reply {
	bits := 256
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|genpass' command")
	}
	if len(args) == 1 {
		n, err := strconv.Atoi(string(args[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if n <= 0 || n > 4096 {
			return protocol.MakeErrReply("ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096")
		}
		bits = n
	}
	// Hex length: ceil(bits/4) characters (Redis ACL GENPASS).
	length := (bits + 3) / 4
	password := generateRandomPassword(length)
	return protocol.MakeBulkReply([]byte(password))
}

// execACLDryRun tests if a user can execute a command
func execACLDryRun(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|dryrun' command")
	}

	if aclEngine == nil {
		return protocol.MakeOkReply()
	}

	username := string(args[0])
	command := string(args[1])

	user, exists := aclEngine.GetUser(username)
	if !exists {
		return protocol.MakeErrReply("ERR User '" + username + "' not found")
	}

	if !user.CheckCommand(command) {
		return protocol.MakeErrReply("ERR this user has no permissions to run the '" + command + "' command")
	}

	// Check key permissions using command prepare keys (Redis ACL DRYRUN + selectors).
	cmdArgs := args[1:] // command + args
	var writeKeys, readKeys []string
	if len(cmdArgs) >= 1 {
		cmdName := strings.ToLower(string(cmdArgs[0]))
		if cmd, ok := cmdTable[cmdName]; ok && cmd.prepare != nil && len(cmdArgs) > 1 {
			writeKeys, readKeys = cmd.prepare(cmdArgs[1:])
		}
	}
	if !user.CheckPermission(command, writeKeys, readKeys) {
		// Narrow error: command allowed in some selector but keys don't match any set.
		for _, k := range writeKeys {
			if !user.CheckPermission(command, []string{k}, nil) {
				return protocol.MakeErrReply("ERR this user has no permissions to access '" + k + "'")
			}
		}
		for _, k := range readKeys {
			if !user.CheckPermission(command, nil, []string{k}) {
				return protocol.MakeErrReply("ERR this user has no permissions to access '" + k + "'")
			}
		}
		return protocol.MakeErrReply("ERR this user has no permissions to run the '" + command + "' command")
	}

	// Channel check for PUBLISH / SUBSCRIBE family when channel args present.
	cmdUpper := strings.ToUpper(command)
	switch cmdUpper {
	case "PUBLISH", "SPUBLISH":
		if len(args) >= 3 {
			ch := string(args[2])
			if !user.CheckChannel(ch) {
				return protocol.MakeErrReply("ERR this user has no permissions to access channel '" + ch + "'")
			}
		}
	case "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE":
		for i := 2; i < len(args); i++ {
			ch := string(args[i])
			if !user.CheckChannel(ch) {
				return protocol.MakeErrReply("ERR this user has no permissions to access channel '" + ch + "'")
			}
		}
	}

	return protocol.MakeOkReply()
}

func execACLSave(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|save' command")
	}
	if aclEngine == nil {
		return protocol.MakeErrReply("ERR ACL engine not initialized")
	}
	path := resolveACLFilePath()
	if path == "" {
		return protocol.MakeErrReply("ERR ACL file not configured")
	}
	if err := aclEngine.SaveToFile(path); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeOkReply()
}

func execACLLoad(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'acl|load' command")
	}
	if aclEngine == nil {
		return protocol.MakeErrReply("ERR ACL engine not initialized")
	}
	path := resolveACLFilePath()
	if path == "" {
		return protocol.MakeErrReply("ERR ACL file not configured")
	}
	if !aclFileExists(path) {
		return protocol.MakeErrReply("ERR ACL file not found")
	}
	if err := aclEngine.LoadFromFile(path); err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	return protocol.MakeOkReply()
}

// formatACLUser formats user as ACL string
func formatACLUser(user *acl.User) []byte {
	return []byte(acl.FormatACLFileLine(user))
}

// formatACLUserReply formats user details as Redis reply.
// RESP3: Map; RESP2: flat field/value array via MapReply.ToBytes.
// Nested arrays (flags/passwords/…) are proper MultiBulk/MultiRaw, not bulk blobs.
func formatACLUserReply(user *acl.User) redis.Reply {
	m := protocol.MakeMapReply()

	flags := [][]byte{}
	if user.Enabled {
		flags = append(flags, []byte("on"))
	} else {
		flags = append(flags, []byte("off"))
	}
	m.Put("flags", protocol.MakeMultiBulkReply(flags))

	var pwdReplies [][]byte
	for _, pwd := range user.Passwords {
		if pwd.IsSHA {
			pwdReplies = append(pwdReplies, []byte("#"+pwd.Hash))
		}
	}
	m.Put("passwords", protocol.MakeMultiBulkReply(pwdReplies))

	var cmdReplies [][]byte
	if user.Commands.AllCommands {
		cmdReplies = append(cmdReplies, []byte("+@all"))
	}
	for cmd := range user.Commands.AllowedCommands {
		cmdReplies = append(cmdReplies, []byte("+"+cmd))
	}
	m.Put("commands", protocol.MakeMultiBulkReply(cmdReplies))

	var keyReplies [][]byte
	for _, kp := range user.KeyPatterns {
		if !kp.Allowed {
			continue
		}
		switch {
		case kp.Read && !kp.Write:
			keyReplies = append(keyReplies, []byte("%R~"+kp.Pattern))
		case kp.Write && !kp.Read:
			keyReplies = append(keyReplies, []byte("%W~"+kp.Pattern))
		default:
			keyReplies = append(keyReplies, []byte("~"+kp.Pattern))
		}
	}
	m.Put("keys", protocol.MakeMultiBulkReply(keyReplies))

	var chReplies [][]byte
	for _, ch := range user.Channels {
		if !ch.Allowed {
			continue
		}
		chReplies = append(chReplies, []byte("&"+ch.Pattern))
	}
	m.Put("channels", protocol.MakeMultiBulkReply(chReplies))

	var selReplies []redis.Reply
	for _, sel := range user.Selectors {
		if sel == nil {
			continue
		}
		sm := protocol.MakeMapReply()
		var cmdParts [][]byte
		if sel.Commands != nil && sel.Commands.AllCommands {
			cmdParts = append(cmdParts, []byte("+@all"))
		}
		if sel.Commands != nil {
			for cmd := range sel.Commands.AllowedCommands {
				cmdParts = append(cmdParts, []byte("+"+cmd))
			}
		}
		sm.Put("commands", protocol.MakeMultiBulkReply(cmdParts))
		var keyParts [][]byte
		for _, kp := range sel.KeyPatterns {
			if !kp.Allowed {
				continue
			}
			keyParts = append(keyParts, []byte("~"+kp.Pattern))
		}
		sm.Put("keys", protocol.MakeMultiBulkReply(keyParts))
		var chParts [][]byte
		for _, ch := range sel.Channels {
			if ch.Allowed {
				chParts = append(chParts, []byte("&"+ch.Pattern))
			}
		}
		sm.Put("channels", protocol.MakeMultiBulkReply(chParts))
		selReplies = append(selReplies, sm)
	}
	m.Put("selectors", protocol.MakeMultiRawReply(selReplies))
	return m
}

// generateRandomPassword generates a cryptographically random hex password
func generateRandomPassword(length int) string {
	const hexchars = "0123456789abcdef"
	buf := make([]byte, length)
	if _, err := rand.Read(buf); err != nil {
		// Extremely unlikely; fall back to zeroed hex rather than a fixed pattern
		for i := range buf {
			buf[i] = '0'
		}
		return string(buf)
	}
	for i := 0; i < length; i++ {
		buf[i] = hexchars[buf[i]%16]
	}
	return string(buf)
}

func init() {
	registerCommand("ACL", execACL, noPrepare, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}
