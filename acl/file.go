package acl

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/linkerlin/godis/datastruct/dict"
)

func sha256SumHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// applyRules applies ACL rules to a user (incremental).
func applyRules(u *User, rules []string) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	i := 0
	for i < len(rules) {
		rule := strings.TrimSpace(rules[i])
		if rule == "" {
			i++
			continue
		}

		if rule == "clearselectors" {
			u.Selectors = nil
			i++
			continue
		}

		// Selector: "(...)" as one token, or "(" ... ")" spanning tokens.
		if strings.HasPrefix(rule, "(") {
			inner, next, err := collectSelectorRules(rules, i)
			if err != nil {
				return err
			}
			sel := newEmptySelector()
			if err := applyRuleList(sel, inner); err != nil {
				return fmt.Errorf("Error in ACL SETUSER modifier '(%s)': %w", strings.Join(inner, " "), err)
			}
			u.Selectors = append(u.Selectors, sel.toSelector())
			i = next
			continue
		}

		if err := applyOneRootRule(u, rule); err != nil {
			return err
		}
		i++
	}
	return nil
}

func collectSelectorRules(rules []string, start int) (inner []string, next int, err error) {
	first := strings.TrimSpace(rules[start])
	if !strings.HasPrefix(first, "(") {
		return nil, start, fmt.Errorf("Unmatched parenthesis in acl selector")
	}
	// Single-token "(+GET ~a*)"
	if strings.HasSuffix(first, ")") && len(first) >= 2 {
		body := strings.TrimSpace(first[1 : len(first)-1])
		if body == "" {
			return nil, start, fmt.Errorf("Syntax error")
		}
		return strings.Fields(body), start + 1, nil
	}
	// Multi-token: "(+PING" "+SELECT" ")"
	depth := 0
	var parts []string
	for i := start; i < len(rules); i++ {
		tok := strings.TrimSpace(rules[i])
		for _, ch := range tok {
			if ch == '(' {
				depth++
			} else if ch == ')' {
				depth--
			}
		}
		if i == start {
			tok = strings.TrimPrefix(tok, "(")
		}
		if depth == 0 {
			tok = strings.TrimSuffix(tok, ")")
			tok = strings.TrimSpace(tok)
			if tok != "" {
				parts = append(parts, strings.Fields(tok)...)
			}
			if len(parts) == 0 {
				return nil, start, fmt.Errorf("Syntax error")
			}
			return parts, i + 1, nil
		}
		if tok != "" {
			parts = append(parts, strings.Fields(tok)...)
		}
	}
	return nil, start, fmt.Errorf("Unmatched parenthesis in acl selector")
}

// ruleSink receives command/key/channel rules (root or selector).
type ruleSink struct {
	Commands    *CommandPermissions
	KeyPatterns *[]KeyPattern
	Channels    *[]ChannelPattern
}

func newEmptySelector() *ruleSink {
	return &ruleSink{
		Commands: &CommandPermissions{
			AllowedCategories: make(map[string]bool),
			AllowedCommands:   make(map[string]bool),
			DeniedCommands:    make(map[string]bool),
		},
		KeyPatterns: &[]KeyPattern{},
		Channels:    &[]ChannelPattern{},
	}
}

func (s *ruleSink) toSelector() *Selector {
	return &Selector{
		Commands:    s.Commands,
		KeyPatterns: *s.KeyPatterns,
		Channels:    *s.Channels,
	}
}

func applyRuleList(s *ruleSink, rules []string) error {
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}
		if err := applyOneSinkRule(s, rule); err != nil {
			return err
		}
	}
	return nil
}

func applyOneRootRule(u *User, rule string) error {
	// Caller must hold u.mu (or be single-threaded via Engine.mu + exclusive user).
	switch {
	case rule == "on":
		u.Enabled = true
	case rule == "off":
		u.Enabled = false
	case rule == "nopass":
		u.Passwords = nil
	case strings.HasPrefix(rule, ">"):
		hash := sha256SumHex(rule[1:])
		u.Passwords = []Password{{Hash: hash, IsSHA: true}}
	case strings.HasPrefix(rule, "#"):
		u.Passwords = []Password{{Hash: rule[1:], IsSHA: true}}
	case rule == "resetkeys":
		u.KeyPatterns = nil
	case rule == "resetchannels":
		u.Channels = nil
	case rule == "allchannels":
		u.Channels = []ChannelPattern{{Pattern: "*", Allowed: true}}
	default:
		sink := &ruleSink{
			Commands:    u.Commands,
			KeyPatterns: &u.KeyPatterns,
			Channels:    &u.Channels,
		}
		return applyOneSinkRule(sink, rule)
	}
	return nil
}

func applyOneSinkRule(s *ruleSink, rule string) error {
	switch {
	case strings.HasPrefix(rule, "+"):
		target := rule[1:]
		if target == "@all" || rule == "+@all" {
			s.Commands.AllCommands = true
		} else if strings.HasPrefix(target, "@") {
			s.Commands.AllowedCategories[target] = true
		} else {
			cmd := strings.ToLower(target)
			s.Commands.AllowedCommands[cmd] = true
			delete(s.Commands.DeniedCommands, cmd)
		}
	case strings.HasPrefix(rule, "-"):
		target := rule[1:]
		if strings.HasPrefix(target, "@") {
			s.Commands.AllowedCategories[target] = false
		} else {
			cmd := strings.ToLower(target)
			s.Commands.DeniedCommands[cmd] = true
			delete(s.Commands.AllowedCommands, cmd)
		}
	case strings.HasPrefix(rule, "~"):
		*s.KeyPatterns = append(*s.KeyPatterns, KeyPattern{
			Pattern: rule[1:], Allowed: true, Read: true, Write: true,
		})
	case strings.HasPrefix(rule, "%RW~"):
		*s.KeyPatterns = append(*s.KeyPatterns, KeyPattern{
			Pattern: rule[4:], Allowed: true, Read: true, Write: true,
		})
	case strings.HasPrefix(rule, "%R~"):
		*s.KeyPatterns = append(*s.KeyPatterns, KeyPattern{
			Pattern: rule[3:], Allowed: true, Read: true, Write: false,
		})
	case strings.HasPrefix(rule, "%W~"):
		*s.KeyPatterns = append(*s.KeyPatterns, KeyPattern{
			Pattern: rule[3:], Allowed: true, Read: false, Write: true,
		})
	case rule == "allkeys" || rule == "~*":
		*s.KeyPatterns = append(*s.KeyPatterns, KeyPattern{
			Pattern: "*", Allowed: true, Read: true, Write: true,
		})
	case rule == "allcommands" || rule == "+@all":
		s.Commands.AllCommands = true
	case strings.HasPrefix(rule, "&"):
		*s.Channels = append(*s.Channels, ChannelPattern{
			Pattern: rule[1:], Allowed: true,
		})
	default:
		return fmt.Errorf("Syntax error")
	}
	return nil
}

func formatKeyPattern(kp KeyPattern) string {
	if !kp.Allowed {
		return "-~" + kp.Pattern
	}
	switch {
	case kp.Read && !kp.Write:
		return "%R~" + kp.Pattern
	case kp.Write && !kp.Read:
		return "%W~" + kp.Pattern
	default:
		return "~" + kp.Pattern
	}
}

func formatCommands(cp *CommandPermissions) []string {
	if cp == nil {
		return nil
	}
	var parts []string
	if cp.AllCommands {
		parts = append(parts, "+@all")
		return parts
	}
	cats := make([]string, 0, len(cp.AllowedCategories))
	for cat, allowed := range cp.AllowedCategories {
		if allowed {
			cats = append(cats, cat)
		}
	}
	sort.Strings(cats)
	for _, cat := range cats {
		parts = append(parts, "+"+cat)
	}
	cmds := make([]string, 0, len(cp.AllowedCommands))
	for cmd := range cp.AllowedCommands {
		cmds = append(cmds, cmd)
	}
	sort.Strings(cmds)
	for _, cmd := range cmds {
		parts = append(parts, "+"+cmd)
	}
	denied := make([]string, 0, len(cp.DeniedCommands))
	for cmd := range cp.DeniedCommands {
		denied = append(denied, cmd)
	}
	sort.Strings(denied)
	for _, cmd := range denied {
		parts = append(parts, "-"+cmd)
	}
	return parts
}

func formatSelector(sel *Selector) string {
	if sel == nil {
		return "()"
	}
	var parts []string
	for _, kp := range sel.KeyPatterns {
		parts = append(parts, formatKeyPattern(kp))
	}
	for _, ch := range sel.Channels {
		if ch.Allowed {
			parts = append(parts, "&"+ch.Pattern)
		}
	}
	parts = append(parts, formatCommands(sel.Commands)...)
	return "(" + strings.Join(parts, " ") + ")"
}

// FormatACLFileLine serializes a user as one Redis-compatible ACL file line.
func FormatACLFileLine(u *User) string {
	u.mu.RLock()
	defer u.mu.RUnlock()

	parts := []string{"user", u.Name}
	if u.Enabled {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}

	if len(u.Passwords) == 0 {
		parts = append(parts, "nopass")
	} else {
		for _, pwd := range u.Passwords {
			parts = append(parts, "#"+pwd.Hash)
		}
	}

	for _, kp := range u.KeyPatterns {
		if kp.Allowed {
			parts = append(parts, formatKeyPattern(kp))
		}
	}
	for _, ch := range u.Channels {
		if ch.Allowed {
			parts = append(parts, "&"+ch.Pattern)
		}
	}
	parts = append(parts, formatCommands(u.Commands)...)
	for _, sel := range u.Selectors {
		parts = append(parts, formatSelector(sel))
	}

	return strings.Join(parts, " ")
}

// ParseACLFile parses Redis ACL file contents into username -> rules.
func ParseACLFile(content string) (map[string][]string, error) {
	users := make(map[string][]string)
	scanner := bufio.NewScanner(strings.NewReader(content))
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.EqualFold(fields[0], "user") {
			return nil, fmt.Errorf("invalid ACL line %d: %q", lineNo, line)
		}
		name := fields[1]
		users[name] = fields[2:]
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(users) == 0 {
		return nil, fmt.Errorf("ACL file contains no users")
	}
	return users, nil
}

// LoadFromFile replaces all ACL users from a Redis ACL file.
func (e *Engine) LoadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	parsed, err := ParseACLFile(string(data))
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.users = dict.MakeConcurrent(16)
	for name, rules := range parsed {
		user := NewUser(name)
		if err := applyRules(user, rules); err != nil {
			return fmt.Errorf("user %s: %w", name, err)
		}
		e.users.Put(name, user)
	}
	return nil
}

// SaveToFile writes all ACL users to a Redis ACL file.
func (e *Engine) SaveToFile(path string) error {
	names := e.GetAllUsers()
	sort.Strings(names)

	lines := make([]string, 0, len(names))
	for _, name := range names {
		user, ok := e.GetUser(name)
		if !ok {
			continue
		}
		lines = append(lines, FormatACLFileLine(user))
	}

	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(path, []byte(content), 0o600)
}
