package redisearch

import "strings"

// Known FT.SEARCH / FT.CREATE LANGUAGE names for Redis 8.10 Query Engine
// (case-insensitive). Empirically verified against redis:8-alpine; matrix
// entries like czech/polish that Redis rejects are intentionally omitted.
var knownSearchLanguages = map[string]struct{}{
	"arabic": {}, "armenian": {}, "basque": {}, "catalan": {}, "chinese": {},
	"danish": {}, "dutch": {}, "english": {}, "finnish": {}, "french": {},
	"german": {}, "greek": {}, "hindi": {}, "hungarian": {}, "indonesian": {},
	"irish": {}, "italian": {}, "lithuanian": {}, "nepali": {}, "norwegian": {},
	"portuguese": {}, "romanian": {}, "russian": {}, "serbian": {},
	"spanish": {}, "swedish": {}, "tamil": {}, "turkish": {}, "yiddish": {},
}

// IsKnownSearchLanguage reports whether name is a Redis 8.x stemmer language
// (case-insensitive). Unknown names → SEARCH_QUERY_BAD No such language.
func IsKnownSearchLanguage(name string) bool {
	_, ok := knownSearchLanguages[strings.ToLower(name)]
	return ok
}
