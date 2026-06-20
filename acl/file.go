package acl

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/linkerlin/godis/datastruct/dict"
)

// applyRules applies ACL rules to a user.
func applyRules(u *User, rules []string) error {
	for _, rule := range rules {
		rule = strings.TrimSpace(rule)
		if rule == "" {
			continue
		}

		switch {
		case rule == "on":
			u.Enabled = true
		case rule == "off":
			u.Enabled = false
		case rule == "nopass":
			u.Passwords = nil
		case strings.HasPrefix(rule, ">"):
			u.SetPassword(rule[1:], false)
		case strings.HasPrefix(rule, "#"):
			u.SetPassword(rule[1:], true)
		case strings.HasPrefix(rule, "+"):
			target := rule[1:]
			if target == "@all" {
				u.AllowAllCommands()
			} else if strings.HasPrefix(target, "@") {
				u.AllowCategory(target)
			} else {
				u.AllowCommand(target)
			}
		case strings.HasPrefix(rule, "-"):
			target := rule[1:]
			if strings.HasPrefix(target, "@") {
				u.DenyCategory(target)
			} else {
				u.DenyCommand(target)
			}
		case strings.HasPrefix(rule, "~"):
			u.AddKeyPattern(rule[1:], true)
		case rule == "allkeys" || rule == "~*":
			u.AddKeyPattern("*", true)
		case rule == "allcommands" || rule == "+@all":
			u.AllowAllCommands()
		case rule == "resetchannels":
			u.Channels = nil
		case strings.HasPrefix(rule, "&"):
			u.Channels = append(u.Channels, ChannelPattern{
				Pattern: rule[1:],
				Allowed: true,
			})
		case rule == "resetkeys":
			u.KeyPatterns = nil
		default:
			return fmt.Errorf("unknown ACL rule: %s", rule)
		}
	}
	return nil
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
			parts = append(parts, "~"+kp.Pattern)
		}
	}
	for _, ch := range u.Channels {
		if ch.Allowed {
			parts = append(parts, "&"+ch.Pattern)
		}
	}

	if u.Commands.AllCommands {
		parts = append(parts, "+@all")
	} else {
		cats := make([]string, 0, len(u.Commands.AllowedCategories))
		for cat, allowed := range u.Commands.AllowedCategories {
			if allowed {
				cats = append(cats, cat)
			}
		}
		sort.Strings(cats)
		for _, cat := range cats {
			parts = append(parts, "+"+cat)
		}

		cmds := make([]string, 0, len(u.Commands.AllowedCommands))
		for cmd := range u.Commands.AllowedCommands {
			cmds = append(cmds, cmd)
		}
		sort.Strings(cmds)
		for _, cmd := range cmds {
			parts = append(parts, "+"+cmd)
		}

		denied := make([]string, 0, len(u.Commands.DeniedCommands))
		for cmd := range u.Commands.DeniedCommands {
			denied = append(denied, cmd)
		}
		sort.Strings(denied)
		for _, cmd := range denied {
			parts = append(parts, "-"+cmd)
		}
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
