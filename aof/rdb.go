package aof

import (
	"os"
	"strconv"
	"time"

	rdb "github.com/hdt3213/rdb/encoder"
	"github.com/hdt3213/rdb/model"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/dict"
	List "github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/set"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/logger"
)

// GenerateRDB generates rdb file from aof file
func (persister *Persister) GenerateRDB(rdbFilename string) error {
	if err := persister.acquireRewriteSlot(); err != nil {
		return err
	}
	defer persister.releaseRewriteSlot()
	return persister.generateRDBToFile(rdbFilename)
}

func (persister *Persister) generateRDBToFile(rdbFilename string) error {
	ctx, err := persister.startGenerateRDB(nil, nil)
	if err != nil {
		return err
	}
	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}

// GenerateRDBForReplication asynchronously generates rdb file from aof file and returns a channel to receive following data
// parameter listener would receive following updates of rdb
// parameter hook allows you to do something during aof pausing
func (persister *Persister) GenerateRDBForReplication(rdbFilename string, listener Listener, hook func()) error {
	if err := persister.acquireRewriteSlot(); err != nil {
		return err
	}
	defer persister.releaseRewriteSlot()

	ctx, err := persister.startGenerateRDB(listener, hook)
	if err != nil {
		return err
	}

	err = persister.generateRDB(ctx)
	if err != nil {
		return err
	}
	err = ctx.tmpFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(ctx.tmpFile.Name(), rdbFilename)
	if err != nil {
		return err
	}
	return nil
}

func (persister *Persister) startGenerateRDB(newListener Listener, hook func()) (*RewriteCtx, error) {
	persister.pausingAof.Lock() // pausing aof
	defer persister.pausingAof.Unlock()

	err := persister.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(persister.aofFilename)
	filesize := fileInfo.Size()
	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	if newListener != nil {
		persister.listeners[newListener] = struct{}{}
	}
	if hook != nil {
		hook()
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
	}, nil
}

// generateRDB generates rdb file from aof file
func (persister *Persister) generateRDB(ctx *RewriteCtx) error {
	// load aof tmpFile
	tmpHandler := persister.newRewriteHandler()
	tmpHandler.LoadAof(int(ctx.fileSize))

	encoder := rdb.NewEncoder(ctx.tmpFile).EnableCompress()
	err := encoder.WriteHeader()
	if err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "8.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}

	// change aof preamble
	if config.Properties.AofUseRdbPreamble {
		auxMap["aof-preamble"] = "1"
	}

	for k, v := range auxMap {
		err := encoder.WriteAux(k, v)
		if err != nil {
			return err
		}
	}

	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount, _ := tmpHandler.db.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		err = encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount))
		if err != nil {
			return err
		}
		// dump db
		var err2 error
		tmpHandler.db.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case List.List:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case *dict.ExpireDict:
				// Redis 8 hash field TTL is not supported by the RDB encoder used here.
				// Fall back to a plain hash and log the limitation.
				logger.Warn("RDB preamble does not preserve hash field TTLs; set aof-use-rdb-preamble=no to keep them")
				hash := make(map[string][]byte)
				obj.ForEach(func(field string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[field] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case dict.Dict:
				hash := make(map[string][]byte)
				obj.ForEach(func(key string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[key] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *SortedSet.SortedSet:
				var entries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *SortedSet.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			default:
				// Godis-specific types (stream/json/vector/timeseries): opaque string blob.
				// Not Redis-interoperable; LoadRDB restores via DecodeOpaque.
				if payload, ok := EncodeOpaque(entity); ok {
					err = encoder.WriteStringObject(key, payload, opts...)
				} else {
					logger.Warn("RDB skip unsupported type for key: " + key)
				}
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	err = encoder.WriteEnd()
	if err != nil {
		return err
	}
	return nil
}

// WriteRDBFromDB writes an RDB snapshot from in-memory databases (no AOF required).
func WriteRDBFromDB(rdbFilename string, db database.DBEngine) error {
	tmpFile, err := os.CreateTemp("", "temp-*.rdb")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	success := false
	defer func() {
		_ = tmpFile.Close()
		if !success {
			_ = os.Remove(tmpName)
		}
	}()

	if err := encodeEngineToRDB(tmpFile, db); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, rdbFilename); err != nil {
		return err
	}
	success = true
	return nil
}

func encodeEngineToRDB(tmpFile *os.File, eng database.DBEngine) error {
	encoder := rdb.NewEncoder(tmpFile).EnableCompress()
	if err := encoder.WriteHeader(); err != nil {
		return err
	}
	auxMap := map[string]string{
		"redis-ver":    "8.0.0",
		"redis-bits":   "64",
		"aof-preamble": "0",
		"ctime":        strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range auxMap {
		if err := encoder.WriteAux(k, v); err != nil {
			return err
		}
	}
	for i := 0; i < config.Properties.Databases; i++ {
		keyCount, ttlCount, _ := eng.GetDBSize(i)
		if keyCount == 0 {
			continue
		}
		if err := encoder.WriteDBHeader(uint(i), uint64(keyCount), uint64(ttlCount)); err != nil {
			return err
		}
		var err2 error
		_ = eng.ForEach(i, func(key string, entity *database.DataEntity, expiration *time.Time) bool {
			var opts []interface{}
			if expiration != nil {
				opts = append(opts, rdb.WithTTL(uint64(expiration.UnixNano()/1e6)))
			}
			var err error
			switch obj := entity.Data.(type) {
			case []byte:
				err = encoder.WriteStringObject(key, obj, opts...)
			case List.List:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(i int, v interface{}) bool {
					bytes, _ := v.([]byte)
					vals = append(vals, bytes)
					return true
				})
				err = encoder.WriteListObject(key, vals, opts...)
			case *set.Set:
				vals := make([][]byte, 0, obj.Len())
				obj.ForEach(func(m string) bool {
					vals = append(vals, []byte(m))
					return true
				})
				err = encoder.WriteSetObject(key, vals, opts...)
			case *dict.ExpireDict:
				hash := make(map[string][]byte)
				obj.ForEach(func(field string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[field] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case dict.Dict:
				hash := make(map[string][]byte)
				obj.ForEach(func(field string, val interface{}) bool {
					bytes, _ := val.([]byte)
					hash[field] = bytes
					return true
				})
				err = encoder.WriteHashMapObject(key, hash, opts...)
			case *SortedSet.SortedSet:
				var entries []*model.ZSetEntry
				obj.ForEachByRank(int64(0), obj.Len(), true, func(element *SortedSet.Element) bool {
					entries = append(entries, &model.ZSetEntry{
						Member: element.Member,
						Score:  element.Score,
					})
					return true
				})
				err = encoder.WriteZSetObject(key, entries, opts...)
			default:
				if payload, ok := EncodeOpaque(entity); ok {
					err = encoder.WriteStringObject(key, payload, opts...)
				}
			}
			if err != nil {
				err2 = err
				return false
			}
			return true
		})
		if err2 != nil {
			return err2
		}
	}
	return encoder.WriteEnd()
}
