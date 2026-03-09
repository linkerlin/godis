package redisearch

import (
	"sort"
	"sync"
)

// Suggestion represents a single autocomplete suggestion
type Suggestion struct {
	Term    string
	Score   float64
	Payload string
}

// Autocomplete provides trie-based autocomplete functionality
type Autocomplete struct {
	mu       sync.RWMutex
	root     *trieNode
	entries  map[string]*Suggestion // Fast lookup for existing entries
}

type trieNode struct {
	children    map[rune]*trieNode
	isEnd       bool
	suggestion  *Suggestion
}

// NewAutocomplete creates a new autocomplete engine
func NewAutocomplete() *Autocomplete {
	return &Autocomplete{
		root:    &trieNode{children: make(map[rune]*trieNode)},
		entries: make(map[string]*Suggestion),
	}
}

// Add adds a suggestion to the autocomplete
func (ac *Autocomplete) Add(term string, score float64, payload string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	// Update or create suggestion
	suggestion := &Suggestion{
		Term:    term,
		Score:   score,
		Payload: payload,
	}
	ac.entries[term] = suggestion
	
	// Add to trie
	node := ac.root
	for _, ch := range term {
		if node.children == nil {
			node.children = make(map[rune]*trieNode)
		}
		if node.children[ch] == nil {
			node.children[ch] = &trieNode{children: make(map[rune]*trieNode)}
		}
		node = node.children[ch]
	}
	node.isEnd = true
	node.suggestion = suggestion
}

// AddIncr adds a suggestion with incrementing score
func (ac *Autocomplete) AddIncr(term string, score float64, payload string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	if existing, ok := ac.entries[term]; ok {
		// Increment score
		existing.Score += score
		if payload != "" {
			existing.Payload = payload
		}
	} else {
		// Add new
		ac.Add(term, score, payload)
	}
}

// Get retrieves suggestions by prefix
func (ac *Autocomplete) Get(prefix string, max int, fuzzy bool) []*Suggestion {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	if max <= 0 {
		max = 5
	}
	
	// Navigate to prefix node
	node := ac.root
	for _, ch := range prefix {
		if node.children[ch] == nil {
			// Prefix not found
			if fuzzy {
				return ac.fuzzySearch(prefix, max)
			}
			return nil
		}
		node = node.children[ch]
	}
	
	// Collect all suggestions under this node
	var results []*Suggestion
	ac.collectSuggestions(node, &results, max)
	
	// Sort by score (descending)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	if len(results) > max {
		results = results[:max]
	}
	
	return results
}

// collectSuggestions recursively collects suggestions from trie
func (ac *Autocomplete) collectSuggestions(node *trieNode, results *[]*Suggestion, max int) {
	if len(*results) >= max {
		return
	}
	
	if node.isEnd && node.suggestion != nil {
		*results = append(*results, node.suggestion)
	}
	
	for _, child := range node.children {
		ac.collectSuggestions(child, results, max)
		if len(*results) >= max {
			return
		}
	}
}

// fuzzySearch performs fuzzy matching for suggestions
func (ac *Autocomplete) fuzzySearch(term string, max int) []*Suggestion {
	var results []*Suggestion
	
	for _, sug := range ac.entries {
		dist := levenshteinDistance(term, sug.Term)
		if dist <= 2 { // Allow up to 2 edits
			results = append(results, sug)
		}
	}
	
	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	if len(results) > max {
		results = results[:max]
	}
	
	return results
}

// Len returns the number of suggestions
func (ac *Autocomplete) Len() int {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return len(ac.entries)
}

// Del deletes a suggestion
func (ac *Autocomplete) Del(term string) bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	
	if _, ok := ac.entries[term]; !ok {
		return false
	}
	
	delete(ac.entries, term)
	
	// Rebuild trie (simpler than removing from trie)
	ac.rebuildTrie()
	
	return true
}

// rebuildTrie rebuilds the trie from entries
func (ac *Autocomplete) rebuildTrie() {
	ac.root = &trieNode{children: make(map[rune]*trieNode)}
	
	for _, suggestion := range ac.entries {
		node := ac.root
		for _, ch := range suggestion.Term {
			if node.children[ch] == nil {
				node.children[ch] = &trieNode{children: make(map[rune]*trieNode)}
			}
			node = node.children[ch]
		}
		node.isEnd = true
		node.suggestion = suggestion
	}
}

// GetAll returns all suggestions
func (ac *Autocomplete) GetAll() []*Suggestion {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	
	results := make([]*Suggestion, 0, len(ac.entries))
	for _, sug := range ac.entries {
		results = append(results, sug)
	}
	
	return results
}
