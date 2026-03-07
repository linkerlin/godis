package redisearch

import (
	"strings"
	"unicode"
)

// Tokenizer splits text into tokens/terms
type Tokenizer interface {
	Tokenize(text string) []string
}

// StandardTokenizer is a simple whitespace and punctuation tokenizer
type StandardTokenizer struct{}

// Tokenize splits text into lowercase tokens
func (t *StandardTokenizer) Tokenize(text string) []string {
	var tokens []string
	var current strings.Builder
	
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			current.WriteRune(unicode.ToLower(r))
		} else if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	
	return tokens
}

// WhitespaceTokenizer only splits on whitespace
type WhitespaceTokenizer struct{}

// Tokenize splits text by whitespace
func (t *WhitespaceTokenizer) Tokenize(text string) []string {
	fields := strings.Fields(text)
	for i, f := range fields {
		fields[i] = strings.ToLower(f)
	}
	return fields
}

// NgramTokenizer creates n-grams from text
type NgramTokenizer struct {
	Min int
	Max int
}

// Tokenize creates n-grams
func (t *NgramTokenizer) Tokenize(text string) []string {
	text = strings.ToLower(text)
	var tokens []string
	
	for n := t.Min; n <= t.Max; n++ {
		for i := 0; i <= len(text)-n; i++ {
			tokens = append(tokens, text[i:i+n])
		}
	}
	
	return tokens
}

// StopWordFilter removes common stop words
type StopWordFilter struct {
	stopWords map[string]bool
}

// NewStopWordFilter creates a filter with English stop words
func NewStopWordFilter() *StopWordFilter {
	stopWords := []string{
		"a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
		"has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
		"to", "was", "will", "with", "the", "this", "but", "they",
		"have", "had", "what", "said", "each", "which", "she", "do",
		"how", "their", "if", "will", "up", "other", "about", "out",
		"many", "then", "them", "these", "so", "some", "her", "would",
		"make", "like", "into", "him", "time", "two", "more", "go",
		"no", "way", "could", "my", "than", "first", "water", "been",
		"call", "who", "its", "now", "find", "long", "down", "day",
		"did", "get", "come", "made", "may", "part", "over", "say",
		"she", "also", "back", "after", "use", "work", "life", "even",
		"new", "want", "here", "look", "too", "more", "very", "when",
		"come", "know", "take", "year", "good", "give", "most", "us",
	}
	
	sw := make(map[string]bool)
	for _, w := range stopWords {
		sw[w] = true
	}
	
	return &StopWordFilter{stopWords: sw}
}

// Filter removes stop words from tokens
func (f *StopWordFilter) Filter(tokens []string) []string {
	var filtered []string
	for _, t := range tokens {
		if !f.stopWords[t] {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

// AddStopWord adds a custom stop word
func (f *StopWordFilter) AddStopWord(word string) {
	f.stopWords[strings.ToLower(word)] = true
}

// Stemmer performs word stemming (Porter Stemmer simplified)
type Stemmer struct{}

// Stem reduces a word to its root form (simplified)
func (s *Stemmer) Stem(word string) string {
	word = strings.ToLower(word)
	
	// Simple suffix rules
	suffixes := []struct {
		suffix string
		repl   string
	}{
		{"ies", "y"},
		{"ied", "y"},
		{"ying", "ie"},
		{"ied", "y"},
		{"ies", "y"},
		{"s", ""},
		{"es", ""},
		{"ed", ""},
		{"ing", ""},
		{"ly", ""},
		{"ment", ""},
		{"ness", ""},
		{"tion", "t"},
		{"sion", "s"},
		{"ity", ""},
		{"er", ""},
		{"or", ""},
		{"ist", ""},
		{"ism", ""},
		{"ize", ""},
		{"ise", ""},
		{"ify", ""},
		{"en", ""},
		{"able", ""},
		{"ible", ""},
		{"al", ""},
		{"ial", ""},
		{"ical", ""},
		{"ful", ""},
		{"less", ""},
		{"ous", ""},
		{"ious", ""},
		{"ative", ""},
		{"itive", ""},
	}
	
	for _, rule := range suffixes {
		if strings.HasSuffix(word, rule.suffix) {
			stem := word[:len(word)-len(rule.suffix)] + rule.repl
			if len(stem) >= 2 {
				return stem
			}
		}
	}
	
	return word
}

// StemAll stems all tokens
func (s *Stemmer) StemAll(tokens []string) []string {
	stemmed := make([]string, len(tokens))
	for i, t := range tokens {
		stemmed[i] = s.Stem(t)
	}
	return stemmed
}
