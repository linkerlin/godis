package database

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/geohash"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// execGeoAdd add a location into SortedSet
// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
func execGeoAdd(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geoadd' command")
	}
	key := string(args[0])
	nx, xx, ch := false, false, false
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "NX":
			nx = true
			i++
		case "XX":
			xx = true
			i++
		case "CH":
			ch = true
			i++
		default:
			goto parseMembers
		}
	}
parseMembers:
	if nx && xx {
		return protocol.MakeErrReply("ERR NX and XX options at the same time are not compatible")
	}
	remaining := args[i:]
	if len(remaining) < 3 || len(remaining)%3 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geoadd' command")
	}
	size := len(remaining) / 3
	elements := make([]*sortedset.Element, 0, size)
	for j := 0; j < size; j++ {
		lngStr := string(remaining[3*j])
		latStr := string(remaining[3*j+1])
		lng, err := strconv.ParseFloat(lngStr, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			return protocol.MakeErrReply("ERR value is not a valid float")
		}
		if lat < -90 || lat > 90 || lng < -180 || lng > 180 {
			// Redis formats the pair with six decimal places (parsed floats), not raw tokens.
			return protocol.MakeErrReply(fmt.Sprintf("ERR invalid longitude,latitude pair %.6f,%.6f", lng, lat))
		}
		code := float64(geohash.Encode(lat, lng))
		elements = append(elements, &sortedset.Element{
			Member: string(remaining[3*j+2]),
			Score:  code,
		})
	}

	sortedSet, _, errReply := db.getOrInitSortedSet(key)
	if errReply != nil {
		return errReply
	}

	changed := int64(0)
	for _, e := range elements {
		old, exists := sortedSet.Get(e.Member)
		if nx && exists {
			continue
		}
		if xx && !exists {
			continue
		}
		isNew := sortedSet.Add(e.Member, e.Score)
		if ch {
			if isNew || (exists && old.Score != e.Score) {
				changed++
			}
		} else if isNew {
			changed++
		}
	}
	db.addAof(utils.ToCmdLine3("geoadd", args...))
	return protocol.MakeIntReply(changed)
}

func undoGeoAdd(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	i := 1
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		if opt == "NX" || opt == "XX" || opt == "CH" {
			i++
			continue
		}
		break
	}
	remaining := args[i:]
	size := len(remaining) / 3
	fields := make([]string, 0, size)
	for j := 0; j < size; j++ {
		fields = append(fields, string(remaining[3*j+2]))
	}
	return rollbackZSetFields(db, key, fields...)
}

// execGeoPos returns location of a member
func execGeoPos(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geopos' command")
	}
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		positions := make([]redis.Reply, len(args)-1)
		for i := range positions {
			positions[i] = protocol.MakeNullBulkReply()
		}
		return protocol.MakeMultiRawReply(positions)
	}

	positions := make([]redis.Reply, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		member := string(args[i+1])
		elem, exists := sortedSet.Get(member)
		if !exists {
			positions[i] = protocol.MakeNullBulkReply()
			continue
		}
		lat, lng := geohash.Decode(uint64(elem.Score))
		lngStr := strconv.FormatFloat(lng, 'f', -1, 64)
		latStr := strconv.FormatFloat(lat, 'f', -1, 64)
		positions[i] = protocol.MakeMultiBulkReply([][]byte{
			[]byte(lngStr), []byte(latStr),
		})
	}
	return protocol.MakeMultiRawReply(positions)
}

// execGeoDist returns the distance between two locations
func execGeoDist(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) != 3 && len(args) != 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geodist' command")
	}
	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return &protocol.NullBulkReply{}
	}

	positions := make([][]float64, 2)
	for i := 1; i < 3; i++ {
		member := string(args[i])
		elem, exists := sortedSet.Get(member)
		if !exists {
			return &protocol.NullBulkReply{}
		}
		lat, lng := geohash.Decode(uint64(elem.Score))
		positions[i-1] = []float64{lat, lng}
	}
	unit := "m"
	if len(args) == 4 {
		unit = strings.ToLower(string(args[3]))
	}
	dis := geohash.Distance(positions[0][0], positions[0][1], positions[1][0], positions[1][1])
	switch unit {
	case "m":
		return protocol.MakeDoubleReply(dis)
	case "km":
		return protocol.MakeDoubleReply(dis / 1000)
	case "mi":
		return protocol.MakeDoubleReply(dis / 1609.34)
	case "ft":
		return protocol.MakeDoubleReply(dis / 0.3048)
	}
	return protocol.MakeErrReply("ERR unsupported unit provided. please use m, km, ft, mi")
}

// execGeoHash return geo-hash-code of given position
func execGeoHash(db *DB, args [][]byte) redis.Reply {
	// parse args
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geohash' command")
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		// Missing key: one nil per requested member (Redis GEOHASH).
		strs := make([][]byte, len(args)-1)
		return protocol.MakeMultiBulkReply(strs)
	}

	strs := make([][]byte, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		member := string(args[i+1])
		elem, exists := sortedSet.Get(member)
		if !exists {
			strs[i] = nil
			continue
		}
		str := geohash.ToString(geohash.FromInt(uint64(elem.Score)))
		strs[i] = []byte(str)
	}
	return protocol.MakeMultiBulkReply(strs)
}

// execGeoRadius returns members within max distance of given point
// GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func execGeoRadius(db *DB, args [][]byte) redis.Reply {
	if len(args) < 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'georadius' command")
	}
	return execGeoRadiusViaSearch(db, args[0], false, args[1], args[2], args[3], args[4], args[5:])
}

// execGeoRadiusByMember returns members within max distance of given member's location
// GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func execGeoRadiusByMember(db *DB, args [][]byte) redis.Reply {
	if len(args) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'georadiusbymember' command")
	}
	return execGeoRadiusViaSearch(db, args[0], true, args[1], nil, args[2], args[3], args[4:])
}

// execGeoRadiusViaSearch maps GEORADIUS* onto GEOSEARCH / GEOSEARCHSTORE (haversine + options).
func execGeoRadiusViaSearch(db *DB, key []byte, fromMember bool, a, b, radius, unit []byte, opts [][]byte) redis.Reply {
	var storeKey, storeDistKey []byte
	hasWith := false
	forward := make([][]byte, 0, 8+len(opts))
	forward = append(forward, key)
	if fromMember {
		forward = append(forward, []byte("FROMMEMBER"), a)
	} else {
		forward = append(forward, []byte("FROMLONLAT"), a, b)
	}
	forward = append(forward, []byte("BYRADIUS"), radius, unit)

	i := 0
	for i < len(opts) {
		opt := strings.ToUpper(string(opts[i]))
		switch opt {
		case "WITHCOORD", "WITHDIST", "WITHHASH", "ASC", "DESC", "ANY":
			if opt == "WITHCOORD" || opt == "WITHDIST" || opt == "WITHHASH" {
				hasWith = true
			}
			forward = append(forward, opts[i])
			i++
		case "COUNT":
			if i+1 >= len(opts) {
				return protocol.MakeSyntaxErrReply()
			}
			forward = append(forward, opts[i], opts[i+1])
			i += 2
			if i < len(opts) && strings.EqualFold(string(opts[i]), "ANY") {
				forward = append(forward, opts[i])
				i++
			}
		case "STORE":
			if i+1 >= len(opts) {
				return protocol.MakeSyntaxErrReply()
			}
			storeKey = opts[i+1]
			i += 2
		case "STOREDIST":
			if i+1 >= len(opts) {
				return protocol.MakeSyntaxErrReply()
			}
			storeDistKey = opts[i+1]
			i += 2
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if storeKey != nil || storeDistKey != nil {
		if hasWith {
			return protocol.MakeErrReply("ERR syntax error")
		}
		if storeKey != nil && storeDistKey != nil {
			return protocol.MakeErrReply("ERR STORE and STOREDIST options are mutually exclusive")
		}
		dest := storeKey
		extra := [][]byte(nil)
		if storeDistKey != nil {
			dest = storeDistKey
			extra = [][]byte{[]byte("STOREDIST")}
		}
		searchArgs := append([][]byte{dest}, forward...)
		searchArgs = append(searchArgs, extra...)
		return execGeoSearchStore(db, searchArgs)
	}

	return execGeoSearch(db, forward)
}

func geoRadius0(sortedSet *sortedset.SortedSet, lat float64, lng float64, radius float64) redis.Reply {
	// Legacy helper (tests); applies haversine filter on geohash neighbour candidates.
	areas := geohash.GetNeighbours(lat, lng, radius)
	seen := make(map[string]struct{})
	var members [][]byte
	for _, area := range areas {
		lower := &sortedset.ScoreBorder{Value: float64(area[0])}
		upper := &sortedset.ScoreBorder{Value: float64(area[1])}
		elements := sortedSet.Range(lower, upper, 0, -1, true)
		for _, elem := range elements {
			if _, ok := seen[elem.Member]; ok {
				continue
			}
			mLat, mLon := extractGeoHash(elem.Score)
			if geohash.Distance(lat, lng, mLat, mLon) > radius {
				continue
			}
			seen[elem.Member] = struct{}{}
			members = append(members, []byte(elem.Member))
		}
	}
	return protocol.MakeMultiBulkReply(members)
}

// execGeoSearch searches for members within a radius or box
// GEOSEARCH key [FROMMEMBER member] [FROMLONLAT lon lat] [BYRADIUS r unit] [BYBOX w h unit] [ASC|DESC] [COUNT count] [WITHCOORD] [WITHDIST] [WITHHASH]
func execGeoSearch(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geosearch' command")
	}

	key := string(args[0])
	sortedSet, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return errReply
	}
	if sortedSet == nil {
		return protocol.MakeEmptyMultiBulkReply()
	}

	// Parse options
	var member string
	var lon, lat float64
	var useMember, useCoord bool
	radius := 0.0
	var unit string
	boxWidth, boxHeight := 0.0, 0.0
	useRadius, useBox := false, false
	asc := true
	count := -1
	anyCount := false
	withCoord, withDist, withHash := false, false, false

	i := 1
	for i < len(args) {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "FROMMEMBER":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			member = string(args[i+1])
			useMember = true
			i += 2
		case "FROMLONLAT":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			lon, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			lat, err = strconv.ParseFloat(string(args[i+2]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			useCoord = true
			i += 3
		case "BYRADIUS":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			radius, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			unit = strings.ToUpper(string(args[i+2]))
			useRadius = true
			i += 3
		case "BYBOX":
			if i+3 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			boxWidth, err = strconv.ParseFloat(string(args[i+1]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			boxHeight, err = strconv.ParseFloat(string(args[i+2]), 64)
			if err != nil {
				return protocol.MakeErrReply("ERR value is not a valid float")
			}
			unit = strings.ToUpper(string(args[i+3]))
			useBox = true
			i += 4
		case "ASC":
			asc = true
			i++
		case "DESC":
			asc = false
			i++
		case "COUNT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			var err error
			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil {
				return protocol.MakeErrReply("ERR value is not an integer or out of range")
			}
			i += 2
			if i < len(args) && strings.EqualFold(string(args[i]), "ANY") {
				anyCount = true
				i++
			}
		case "ANY":
			anyCount = true
			i++
		case "WITHCOORD":
			withCoord = true
			i++
		case "WITHDIST":
			withDist = true
			i++
		case "WITHHASH":
			withHash = true
			i++
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	// Get center point
	if useMember {
		elem, exists := sortedSet.Get(member)
		if !exists {
			return protocol.MakeErrReply("ERR could not decode requested zset member")
		}
		lat, lon = extractGeoHash(elem.Score)
	} else if !useCoord {
		return protocol.MakeErrReply("ERR need FROMMEMBER or FROMLONLAT")
	}

	// Convert unit to meters
	var unitMultiplier float64
	switch unit {
	case "M":
		unitMultiplier = 1.0
	case "KM":
		unitMultiplier = 1000.0
	case "MI":
		unitMultiplier = 1609.34
	case "FT":
		unitMultiplier = 0.3048
	default:
		return protocol.MakeErrReply("ERR unsupported unit provided. please use M, KM, MI or FT")
	}

	// Get all members and filter
	var results []geoSearchResult
	allMembers := sortedSet.RangeByRank(0, sortedSet.Len(), false)

	for _, elem := range allMembers {
		mLat, mLon := extractGeoHash(elem.Score)

		var include bool
		if useRadius {
			dist := geohash.Distance(lat, lon, mLat, mLon)
			include = dist <= radius*unitMultiplier
			if include {
				results = append(results, geoSearchResult{
					member: elem.Member,
					dist:   dist / unitMultiplier,
					hash:   elem.Score,
					lat:    mLat,
					lon:    mLon,
				})
			}
		} else if useBox {
			dLon := geohash.Distance(0, mLon, 0, lon)
			dLat := geohash.Distance(mLat, 0, lat, 0)
			include = dLon <= boxWidth*unitMultiplier/2 && dLat <= boxHeight*unitMultiplier/2
			if include {
				dist := geohash.Distance(lat, lon, mLat, mLon)
				results = append(results, geoSearchResult{
					member: elem.Member,
					dist:   dist / unitMultiplier,
					hash:   elem.Score,
					lat:    mLat,
					lon:    mLon,
				})
			}
		}
	}

	// Sort results (ANY skips distance ordering once enough matches exist)
	if !anyCount {
		sort.Slice(results, func(i, j int) bool {
			if asc {
				return results[i].dist < results[j].dist
			}
			return results[i].dist > results[j].dist
		})
	}

	// Apply count limit
	if count > 0 && count < len(results) {
		results = results[:count]
	}

	// Build reply
	var reply []redis.Reply
	for _, r := range results {
		memberReply := []redis.Reply{protocol.MakeBulkReply([]byte(r.member))}

		if withDist {
			distStr := strconv.FormatFloat(r.dist, 'f', -1, 64)
			memberReply = append(memberReply, protocol.MakeBulkReply([]byte(distStr)))
		}

		if withHash {
			hashStr := strconv.FormatInt(int64(r.hash), 10)
			memberReply = append(memberReply, protocol.MakeBulkReply([]byte(hashStr)))
		}

		if withCoord {
			coordReply := []redis.Reply{
				protocol.MakeBulkReply([]byte(strconv.FormatFloat(r.lon, 'f', -1, 64))),
				protocol.MakeBulkReply([]byte(strconv.FormatFloat(r.lat, 'f', -1, 64))),
			}
			memberReply = append(memberReply, protocol.MakeMultiRawReply(coordReply))
		}

		if len(memberReply) == 1 {
			reply = append(reply, memberReply[0])
		} else {
			reply = append(reply, protocol.MakeMultiRawReply(memberReply))
		}
	}

	if !withCoord && !withDist && !withHash {
		args := make([][]byte, len(reply))
		for i, r := range reply {
			br, ok := r.(*protocol.BulkReply)
			if !ok {
				return protocol.MakeMultiRawReply(reply)
			}
			args[i] = br.Arg
		}
		return protocol.MakeMultiBulkReply(args)
	}
	return protocol.MakeMultiRawReply(reply)
}

type geoSearchResult struct {
	member string
	dist   float64
	hash   float64
	lat    float64
	lon    float64
}

// execGeoSearchStore searches and stores results
// GEOSEARCHSTORE destination source [FROMMEMBER member] [FROMLONLAT lon lat] [BYRADIUS r unit] [BYBOX w h unit] [ASC|DESC] [COUNT count] [STOREDIST]
func execGeoSearchStore(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'geosearchstore' command")
	}

	destKey := string(args[0])
	storeDist := false
	searchArgs := make([][]byte, 0, len(args)-1)
	searchArgs = append(searchArgs, args[1]) // source key
	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "STOREDIST":
			storeDist = true
		case "WITHCOORD", "WITHDIST", "WITHHASH":
			return protocol.MakeErrReply("ERR syntax error")
		default:
			searchArgs = append(searchArgs, args[i])
		}
	}

	// Force WITHDIST+WITHHASH so we can recover score/distance from nested replies
	searchWithMeta := append(append([][]byte{}, searchArgs...), []byte("WITHDIST"), []byte("WITHHASH"))
	searchResult := execGeoSearch(db, searchWithMeta)
	if protocol.IsErrorReply(searchResult) {
		return searchResult
	}

	multiReply, ok := searchResult.(*protocol.MultiRawReply)
	if !ok {
		return protocol.MakeIntReply(0)
	}

	newSet := sortedset.Make()
	for _, r := range multiReply.Replies {
		nested, ok := r.(*protocol.MultiRawReply)
		if !ok || len(nested.Replies) < 3 {
			continue
		}
		memberBulk, ok1 := nested.Replies[0].(*protocol.BulkReply)
		distBulk, ok2 := nested.Replies[1].(*protocol.BulkReply)
		hashBulk, ok3 := nested.Replies[2].(*protocol.BulkReply)
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		member := string(memberBulk.Arg)
		var score float64
		if storeDist {
			score, _ = strconv.ParseFloat(string(distBulk.Arg), 64)
		} else {
			score, _ = strconv.ParseFloat(string(hashBulk.Arg), 64)
		}
		newSet.Add(member, score)
	}

	if newSet.Len() == 0 {
		db.Remove(destKey)
	} else {
		db.PutEntity(destKey, &database.DataEntity{Data: newSet})
	}
	db.addAof(utils.ToCmdLine3("geosearchstore", args...))

	return protocol.MakeIntReply(int64(newSet.Len()))
}

// extractGeoHash extracts latitude and longitude from a geohash score
func extractGeoHash(score float64) (float64, float64) {
	return geohash.Decode(uint64(score))
}

func geoUnitToMeters(radius float64, unit string) (float64, redis.Reply) {
	switch strings.ToLower(unit) {
	case "m":
		return radius, nil
	case "km":
		return radius * 1000, nil
	case "mi":
		return radius * 1609.34, nil
	case "ft":
		return radius * 0.3048, nil
	default:
		return 0, protocol.MakeErrReply("ERR unsupported unit provided. please use m, km, ft, mi")
	}
}

// prepareWriteKeys prepares write keys for commands
func prepareWriteKeys(args [][]byte) ([]string, []string) {
	return []string{string(args[0])}, nil
}

func init() {
	registerCommand("GeoAdd", execGeoAdd, writeFirstKey, undoGeoAdd, -5, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
	registerCommand("GeoPos", execGeoPos, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("GeoDist", execGeoDist, readFirstKey, nil, -4, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("GeoHash", execGeoHash, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("GeoRadius", execGeoRadius, readFirstKey, nil, -6, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagMovableKeys}, 1, 1, 1)
	registerCommand("GeoRadiusByMember", execGeoRadiusByMember, readFirstKey, nil, -5, flagReadOnly).
		attachCommandExtra([]string{redisFlagWrite, redisFlagMovableKeys}, 1, 1, 1)
	registerCommand("GeoSearch", execGeoSearch, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 1, 1, 1)
	registerCommand("GeoSearchStore", execGeoSearchStore, prepareGeoSearchStore, rollbackFirstKey, -3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite, redisFlagDenyOOM}, 1, 1, 1)
}

// prepareGeoSearchStore write-locks destination and read-locks source.
func prepareGeoSearchStore(args [][]byte) ([]string, []string) {
	if len(args) < 2 {
		return nil, nil
	}
	return []string{string(args[0])}, []string{string(args[1])}
}
