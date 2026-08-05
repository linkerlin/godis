package database

import (
	"strconv"
	"strings"

	database2 "github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execFTSpellCheck performs spell checking on query terms
// FT.SPELLCHECK index query [DISTANCE dist] [TERMS {INCLUDE | EXCLUDE} {dict} [TERMS {INCLUDE | EXCLUDE} {dict}...]
func execFTSpellCheck(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.spellcheck' command")
	}

	indexName := resolveSearchIndex(string(args[0]))
	query := string(args[1])

	// Parse options
	distance := 1
	includeDicts := []string{}
	excludeDicts := []string{}

	for i := 2; i < len(args); {
		arg := strings.ToUpper(string(args[i]))

		switch arg {
		case "DISTANCE":
			if i+1 < len(args) {
				d, err := strconv.Atoi(string(args[i+1]))
				if err != nil || d < 0 {
					return protocol.MakeErrReply("ERR Invalid DISTANCE")
				}
				if d > 0 {
					distance = d
				}
				i += 2
			} else {
				i++
			}
		case "TERMS":
			if i+2 < len(args) {
				mode := strings.ToUpper(string(args[i+1]))
				dict := string(args[i+2])

				switch mode {
				case "INCLUDE":
					includeDicts = append(includeDicts, dict)
				case "EXCLUDE":
					excludeDicts = append(excludeDicts, dict)
				default:
					return protocol.MakeErrReply("ERR Unmatched argument " + string(args[i+1]))
				}
				i += 3
			} else {
				i++
			}
		case "DIALECT":
			// DIALECT is accepted for syntax parity; spellcheck is dialect-agnostic.
			if i+1 < len(args) {
				i += 2
			} else {
				i++
			}
		default:
			i++
		}
	}

	// Resolve engine via the canonical store (alias-aware).
	searchEnginesMu.RLock()
	engine, ok := searchEngines[indexName]
	searchEnginesMu.RUnlock()
	if !ok {
		return protocol.MakeErrReply("ERR Unknown Index name")
	}

	// Load INCLUDE / EXCLUDE term sets from the __dict_<name> keys.
	loadDict := func(name string) map[string]bool {
		entity, exists := db.GetEntity("__dict_" + name)
		if !exists {
			return nil
		}
		d, ok := entity.Data.(map[string]bool)
		if !ok {
			return nil
		}
		return d
	}
	var includeTerms, excludeTerms map[string]bool
	for _, d := range includeDicts {
		if t := loadDict(d); t != nil {
			if includeTerms == nil {
				includeTerms = make(map[string]bool)
			}
			for k := range t {
				includeTerms[k] = true
			}
		}
	}
	for _, d := range excludeDicts {
		if t := loadDict(d); t != nil {
			if excludeTerms == nil {
				excludeTerms = make(map[string]bool)
			}
			for k := range t {
				excludeTerms[k] = true
			}
		}
	}

	// Parse query and check each term
	terms := parseQueryTerms(query)

	var result [][]byte

	for _, term := range terms {
		if term == "" {
			continue
		}

		// Skip if term exists in index (it's spelled correctly).
		if engine.TermExists(term) {
			continue
		}

		suggestions := engine.SpellCheckWithDicts(term, distance, includeTerms, excludeTerms)
		if len(suggestions) == 0 {
			continue
		}

		// Build term result: ENTRY <term> <score> <suggestion> [<score> <suggestion> ...]
		var termResult [][]byte
		termResult = append(termResult, []byte(term))

		for i, suggestion := range suggestions {
			if i >= 5 { // Limit to 5 suggestions
				break
			}

			var suggResult [][]byte
			suggResult = append(suggResult, []byte(strconv.FormatFloat(suggestion.Score, 'f', -1, 64)))
			suggResult = append(suggResult, []byte(suggestion.Term))

			termResult = append(termResult, protocol.MakeMultiBulkReply(suggResult).ToBytes())
		}

		result = append(result, protocol.MakeMultiBulkReply(termResult).ToBytes())
	}

	return protocol.MakeMultiBulkReply(result)
}

// parseQueryTerms extracts terms from a query
func parseQueryTerms(query string) []string {
	// Simple term extraction
	var terms []string

	// Remove special characters
	query = strings.ReplaceAll(query, "@", " ")
	query = strings.ReplaceAll(query, "(", " ")
	query = strings.ReplaceAll(query, ")", " ")
	query = strings.ReplaceAll(query, "[", " ")
	query = strings.ReplaceAll(query, "]", " ")
	query = strings.ReplaceAll(query, "{", " ")
	query = strings.ReplaceAll(query, "}", " ")
	query = strings.ReplaceAll(query, "|", " ")
	query = strings.ReplaceAll(query, "&", " ")

	// Split by whitespace
	words := strings.Fields(query)

	for _, word := range words {
		word = strings.TrimSpace(word)
		word = strings.Trim(word, "'\"")

		// Skip operators and empty
		if word == "" || word == "OR" || word == "AND" || word == "NOT" {
			continue
		}

		// Skip wildcards
		if strings.Contains(word, "*") || strings.Contains(word, "?") {
			continue
		}

		terms = append(terms, strings.ToLower(word))
	}

	return terms
}

// execFTDictAdd adds terms to a dictionary
// FT.DICTADD dict term [term ...]
func execFTDictAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.dictadd' command")
	}

	dictName := string(args[0])

	// Get or create dictionary
	key := "__dict_" + dictName

	entity, exists := db.GetEntity(key)
	var dict map[string]bool

	if !exists {
		dict = make(map[string]bool)
		db.PutEntity(key, &database2.DataEntity{Data: dict})
	} else {
		var ok bool
		dict, ok = entity.Data.(map[string]bool)
		if !ok {
			dict = make(map[string]bool)
		}
	}

	// Add terms
	added := 0
	for i := 1; i < len(args); i++ {
		term := string(args[i])
		if _, ok := dict[term]; !ok {
			dict[term] = true
			added++
		}
	}
	// Persist so the dictionary survives AOF replay / restart.
	if added > 0 {
		db.addAof(utils.ToCmdLine3("ft.dictadd", args...))
	}
	return protocol.MakeIntReply(int64(added))
}

// execFTDictDel deletes terms from a dictionary
// FT.DICTDEL dict term [term ...]
func execFTDictDel(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.dictdel' command")
	}

	dictName := string(args[0])
	key := "__dict_" + dictName

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	dict, ok := entity.Data.(map[string]bool)
	if !ok {
		return protocol.MakeIntReply(0)
	}

	// Delete terms
	deleted := 0
	for i := 1; i < len(args); i++ {
		term := string(args[i])
		if _, ok := dict[term]; ok {
			delete(dict, term)
			deleted++
		}
	}
	if deleted > 0 {
		db.addAof(utils.ToCmdLine3("ft.dictdel", args...))
	}
	return protocol.MakeIntReply(int64(deleted))
}

// execFTDictDump dumps all terms in a dictionary
// FT.DICTDUMP dict
func execFTDictDump(db *DB, args [][]byte) redis.Reply {
	if len(args) != 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ft.dictdump' command")
	}

	dictName := string(args[0])
	key := "__dict_" + dictName

	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeEmptyMultiBulkReply()
	}

	dict, ok := entity.Data.(map[string]bool)
	if !ok {
		return protocol.MakeEmptyMultiBulkReply()
	}

	var terms [][]byte
	for term := range dict {
		terms = append(terms, []byte(term))
	}

	return protocol.MakeMultiBulkReply(terms)
}

func init() {
	registerCommand("FT.SpellCheck", execFTSpellCheck, readFirstKey, nil, -3, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("FT.DictAdd", execFTDictAdd, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.DictDel", execFTDictDel, writeFirstKey, nil, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 1, 1, 1)
	registerCommand("FT.DictDump", execFTDictDump, readFirstKey, nil, 2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
}
