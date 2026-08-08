package database

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/hdt3213/rdb/core"
	rdb "github.com/hdt3213/rdb/parser"
	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/dict"
	List "github.com/linkerlin/godis/datastruct/list"
	HashSet "github.com/linkerlin/godis/datastruct/set"
	SortedSet "github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/lib/logger"
)

// loadRdbFile loads rdb file from disk
func (server *Server) loadRdbFile() error {
	rdbFile, err := os.Open(config.Properties.RDBFilename)
	if err != nil {
		// Preserve os.IsNotExist so callers can treat first-start as non-fatal.
		if os.IsNotExist(err) {
			return err
		}
		return fmt.Errorf("open rdb file failed: %w", err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	decoder := rdb.NewDecoder(rdbFile)
	err = server.LoadRDB(decoder)
	if err != nil {
		return fmt.Errorf("load rdb file failed: %w", err)
	}
	return nil
}

// LoadRDB real implementation of loading rdb file
func (server *Server) LoadRDB(dec *core.Decoder) error {
	return dec.Parse(func(o rdb.RedisObject) bool {
		db, err := server.selectDBSafe(o.GetDBIndex())
		if err != nil {
			logger.Warn(fmt.Sprintf("rdb: skip object key=%q db=%d: %v", o.GetKey(), o.GetDBIndex(), err))
			return true
		}
		var entity *database.DataEntity
		switch o.GetType() {
		case rdb.StringType:
			str := o.(*rdb.StringObject)
			if restored, ok := aof.DecodeOpaque(str.Value); ok {
				entity = restored
			} else {
				entity = &database.DataEntity{
					Data: str.Value,
				}
			}
		case rdb.ListType:
			listObj := o.(*rdb.ListObject)
			list := List.NewQuickList()
			for _, v := range listObj.Values {
				list.Add(v)
			}
			entity = &database.DataEntity{
				Data: list,
			}
		case rdb.HashType:
			hashObj := o.(*rdb.HashObject)
			hash := dict.MakeSimple()
			for k, v := range hashObj.Hash {
				hash.Put(k, v)
			}
			entity = &database.DataEntity{
				Data: hash,
			}
		case rdb.SetType:
			setObj := o.(*rdb.SetObject)
			set := HashSet.Make()
			for _, mem := range setObj.Members {
				set.Add(string(mem))
			}
			entity = &database.DataEntity{
				Data: set,
			}
		case rdb.ZSetType:
			zsetObj := o.(*rdb.ZSetObject)
			zSet := SortedSet.Make()
			for _, e := range zsetObj.Entries {
				zSet.Add(e.Member, e.Score)
			}
			entity = &database.DataEntity{
				Data: zSet,
			}
		}
		if entity != nil {
			db.PutEntity(o.GetKey(), entity)
			if o.GetExpiration() != nil {
				db.Expire(o.GetKey(), *o.GetExpiration())
			}
			if cmd := aof.EntityToCmd(o.GetKey(), entity); cmd != nil {
				db.addAof(cmd.Args)
			}
		}
		return true
	})
}

func NewPersister(db database.DBEngine, filename string, load bool, fsync string) (*aof.Persister, error) {
	return aof.NewPersister(db, filename, load, fsync, func() database.DBEngine {
		return MakeAuxiliaryServer()
	})
}

func (server *Server) AddAof(dbIndex int, cmdLine CmdLine) {
	if server.persister != nil {
		server.persister.SaveCmdLine(dbIndex, cmdLine)
	}
}

func (server *Server) bindPersister(persister *aof.Persister) {
	server.persister = persister
	// bind SaveCmdLine
	for _, db := range server.dbSet {
		singleDB := db.Load().(*DB)
		singleDB.addAof = func(line CmdLine) {
			if config.Properties.AppendOnly { // config may be changed during runtime
				server.persister.SaveCmdLine(singleDB.index, line)
			}
		}
	}
}

// MakeAuxiliaryServer create a Server only with basic capabilities for aof rewrite and other usages
func MakeAuxiliaryServer() *Server {
	mdb := &Server{}
	mdb.dbSet = make([]*atomic.Value, config.Properties.Databases)
	for i := range mdb.dbSet {
		holder := &atomic.Value{}
		holder.Store(makeBasicDB())
		mdb.dbSet[i] = holder
	}
	return mdb
}
