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
	Timestamp  time.Time
}

// ACL日志存储
var (
	aclLogEntries []*ACLLogEntry
	aclLogMu      sync.RWMutex
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
func addACLLogEntry(reason, context, object, username string) {
	aclLogMu.Lock()
	defer aclLogMu.Unlock()

	// 检查是否存在相同条目（去重）
	for _, entry := range aclLogEntries {
		if entry.Reason == reason && entry.Object == object && entry.Username == username {
			entry.Count++
			entry.Timestamp = time.Now()
			return
		}
	}

	// 添加新条目
	entry := &ACLLogEntry{
		Count:     1,
		Reason:    reason,
		Context:   context,
		Object:    object,
		Username:  username,
		Timestamp: time.Now(),
	}

	aclLogEntries = append(aclLogEntries, entry)

	// 限制日志数量
	maxLen := getACLLogMaxLen()
	if len(aclLogEntries) > maxLen {
		aclLogEntries = aclLogEntries[len(aclLogEntries)-maxLen:]
	}
}

// getACLLogEntries 获取ACL日志条目
func getACLLogEntries(count int) redis.Reply {
	aclLogMu.RLock()
	defer aclLogMu.RUnlock()

	entries := aclLogEntries
	if count > 0 && count < len(entries) {
		entries = entries[len(entries)-count:]
	}

	var result [][]byte
	now := time.Now()

	for _, entry := range entries {
		age := now.Sub(entry.Timestamp).Seconds()

		var fields [][]byte
		fields = append(fields, []byte("count"))
		fields = append(fields, []byte(strconv.FormatInt(entry.Count, 10)))
		fields = append(fields, []byte("reason"))
		fields = append(fields, []byte(entry.Reason))
		fields = append(fields, []byte("context"))
		fields = append(fields, []byte(entry.Context))
		fields = append(fields, []byte("object"))
		fields = append(fields, []byte(entry.Object))
		fields = append(fields, []byte("username"))
		fields = append(fields, []byte(entry.Username))
		fields = append(fields, []byte("age-seconds"))
		fields = append(fields, []byte(strconv.FormatFloat(age, 'f', 6, 64)))
		fields = append(fields, []byte("timestamp"))
		fields = append(fields, []byte(strconv.FormatInt(entry.Timestamp.Unix(), 10)))

		result = append(result, protocol.MakeMultiBulkReply(fields).ToBytes())
	}

	return protocol.MakeMultiBulkReply(result)
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
	if aclEngine == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	if len(args) == 0 {
		// List all categories
		categories := []string{
			"@keyspace", "@read", "@write", "@set", "@sortedset", "@list", "@hash",
			"@string", "@bitmap", "@hyperloglog", "@geo", "@stream", "@pubsub",
			"@admin", "@fast", "@slow", "@blocking", "@dangerous", "@connection",
			"@transaction", "@scripting", "@all",
			"@json", "@search", "@vector", "@bloom", "@cuckoo", "@timeseries",
			"@cms", "@topk", "@tdigest",
		}
		result := make([][]byte, len(categories))
		for i, cat := range categories {
			result[i] = []byte(cat)
		}
		return protocol.MakeMultiBulkReply(result)
	}

	// List commands in a category
	category := strings.ToLower(string(args[0]))
	if !strings.HasPrefix(category, "@") {
		category = "@" + category
	}
	cmds := commandsForACLCategory(category)
	if cmds == nil {
		return protocol.MakeErrReply("ERR Unknown category '" + string(args[0]) + "'")
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
		// 返回所有日志条目
		return getACLLogEntries(-1)
	}

	// 检查RESET
	if len(args) == 1 && strings.ToUpper(string(args[0])) == "RESET" {
		resetACLLog()
		return protocol.MakeOkReply()
	}

	// 解析数量限制
	count := -1
	if len(args) >= 1 {
		c, err := strconv.Atoi(string(args[0]))
		if err == nil && c > 0 {
			count = c
		}
	}

	return getACLLogEntries(count)
}

// execACLHelp returns help information
func execACLHelp(args [][]byte) redis.Reply {
	help := []string{
		"ACL (<subcommand> [<arg> [value] [opt] ...])",
		"Subcommands:",
		"CAT [<category>]",
		"    Return the categories or commands within a category.",
		"DELUSER <username> [<username> ...]",
		"    Delete the specified ACL users and terminate their connections.",
		"DRYRUN <username> <command> [<arg> ...]",
		"    Returns whether the user can execute the given command without executing it.",
		"GETUSER <username>",
		"    Return the rules defined for an ACL user.",
		"GENPASS [<bits>]",
		"    Generate a secure pseudorandom password.",
		"LIST",
		"    Return the currently active ACL rules.",
		"LOG [<count> | RESET]",
		"    Return the latest ACL log entries or reset the log.",
		"SETUSER <username> <rule> [<rule> ...]",
		"    Modify or create the rules for a specific ACL user.",
		"SAVE",
		"    Save the current ACL rules to the configured aclfile.",
		"LOAD",
		"    Reload ACL rules from the configured aclfile.",
		"USERS",
		"    Return the currently active usernames.",
		"WHOAMI",
		"    Return the username the current connection is authenticated with.",
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
		if err != nil || n <= 0 || n > 4096 || n%4 != 0 {
			return protocol.MakeErrReply("ERR The size of the password should be a multiple of 4 between 4 and 4096")
		}
		bits = n
	}
	// Each hex char is 4 bits
	length := bits / 4
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

// formatACLUserReply formats user details as Redis reply
func formatACLUserReply(user *acl.User) redis.Reply {
	var result [][]byte

	// Flags
	flags := []string{}
	if user.Enabled {
		flags = append(flags, "on")
	} else {
		flags = append(flags, "off")
	}

	var flagReplies [][]byte
	for _, f := range flags {
		flagReplies = append(flagReplies, []byte(f))
	}
	result = append(result, []byte("flags"))
	result = append(result, protocol.MakeMultiBulkReply(flagReplies).ToBytes())

	// Passwords
	result = append(result, []byte("passwords"))
	var pwdReplies [][]byte
	for _, pwd := range user.Passwords {
		if pwd.IsSHA {
			pwdReplies = append(pwdReplies, []byte("#"+pwd.Hash))
		}
	}
	result = append(result, protocol.MakeMultiBulkReply(pwdReplies).ToBytes())

	// Commands
	result = append(result, []byte("commands"))
	var cmdReplies [][]byte
	if user.Commands.AllCommands {
		cmdReplies = append(cmdReplies, []byte("+@all"))
	}
	for cmd := range user.Commands.AllowedCommands {
		cmdReplies = append(cmdReplies, []byte("+"+cmd))
	}
	result = append(result, protocol.MakeMultiBulkReply(cmdReplies).ToBytes())

	// Keys
	result = append(result, []byte("keys"))
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
	result = append(result, protocol.MakeMultiBulkReply(keyReplies).ToBytes())

	// Channels (&pattern)
	result = append(result, []byte("channels"))
	var chReplies [][]byte
	for _, ch := range user.Channels {
		if !ch.Allowed {
			continue
		}
		chReplies = append(chReplies, []byte("&"+ch.Pattern))
	}
	result = append(result, protocol.MakeMultiBulkReply(chReplies).ToBytes())

	// Selectors (Redis 7+); empty array when none
	result = append(result, []byte("selectors"))
	var selNested [][]byte
	for _, sel := range user.Selectors {
		if sel == nil {
			continue
		}
		var parts [][]byte
		parts = append(parts, []byte("commands"))
		var cmdParts [][]byte
		if sel.Commands != nil && sel.Commands.AllCommands {
			cmdParts = append(cmdParts, []byte("+@all"))
		}
		if sel.Commands != nil {
			for cmd := range sel.Commands.AllowedCommands {
				cmdParts = append(cmdParts, []byte("+"+cmd))
			}
		}
		parts = append(parts, protocol.MakeMultiBulkReply(cmdParts).ToBytes())
		parts = append(parts, []byte("keys"))
		var keyParts [][]byte
		for _, kp := range sel.KeyPatterns {
			if !kp.Allowed {
				continue
			}
			keyParts = append(keyParts, []byte("~"+kp.Pattern))
		}
		parts = append(parts, protocol.MakeMultiBulkReply(keyParts).ToBytes())
		parts = append(parts, []byte("channels"))
		var chParts [][]byte
		for _, ch := range sel.Channels {
			if ch.Allowed {
				chParts = append(chParts, []byte("&"+ch.Pattern))
			}
		}
		parts = append(parts, protocol.MakeMultiBulkReply(chParts).ToBytes())
		selNested = append(selNested, protocol.MakeMultiBulkReply(parts).ToBytes())
	}
	result = append(result, protocol.MakeMultiBulkReply(selNested).ToBytes())

	return protocol.MakeMultiBulkReply(result)
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
