package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/utils"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/protobuf/proto"
)

const (
	LastBlockInfo = "rpl_last_bk"
	DBInfo        = "rpl_db_info"
)

type dbWrap struct {
	id     int32
	db     DB
	path   string
	dbType string
	isMeta bool
}

// DBPool is a pool of Chain's All DBs.
type DBPool struct {
	sync.RWMutex
	dbs      map[int32]dbWrap
	metaDBID int32
}

func NewDBPool() *DBPool {
	return &DBPool{
		dbs:      make(map[int32]dbWrap),
		metaDBID: math.MinInt32,
	}
}

// RegisterMetaDB registers a DB.
// if isMetaDB is true, this DB will be used to store msgHead.
func (p *DBPool) Register(id int32, path string, dbType string, db DB, isMetaDB bool) {
	p.Lock()
	defer p.Unlock()
	p.dbs[id] = dbWrap{
		id:     id,
		db:     db,
		path:   path,
		dbType: dbType,
		isMeta: isMetaDB,
	}
	if isMetaDB && p.metaDBID != math.MinInt32 {
		panic("meta db already registered")
	}
	if isMetaDB {
		p.metaDBID = id
	}
}

// Open opens DB
func (p *DBPool) Open(dbInfo *pb.DBInfo, cacheSize int) (err error) {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.dbs[dbInfo.Id]; ok {
		return nil
	}
	switch dbInfo.DbType {
	case "leveldb":
		for _, db := range p.dbs {
			if db.path == dbInfo.DbPath {
				return nil
			}
		}
		db, err := NewLDB(dbInfo.DbPath, cacheSize)
		if err != nil {
			return err
		}
		p.dbs[dbInfo.Id] = dbWrap{
			id:     dbInfo.Id,
			db:     db,
			path:   dbInfo.DbPath,
			dbType: dbInfo.DbType,
			isMeta: dbInfo.IsMeta,
		}
		if dbInfo.IsMeta && p.metaDBID != math.MinInt32 {
			return utils.ErrMetaDBAlreadyRegistered
		}
		if dbInfo.IsMeta {
			p.metaDBID = dbInfo.Id
		}
	}
	return nil
}

// GetBlockInfo returns the last BlockInfo from metaDB.
func (p *DBPool) GetBlockInfo() (header *pb.BlockInfo, err error) {
	p.RLock()
	defer p.RUnlock()
	if p.metaDBID == math.MinInt32 {
		return nil, utils.ErrNoMetaDBRegistered
	}
	db := p.dbs[p.metaDBID]
	lastMsgHeadBuf, err := db.db.Get([]byte(LastBlockInfo))
	if err != nil && err != leveldb.ErrNotFound {
		return nil, err
	}
	header = &pb.BlockInfo{
		BlockNum:  -1,
		MsgOffset: -1,
	}
	if header.BlockNum == -1 {
		blockNum, blockHash, err := getRawBlockInfoFromDB(db.db)
		if err != nil {
			return nil, err
		}
		header.BlockNum = blockNum
		header.BlockHash = blockHash
	}
	if err == leveldb.ErrNotFound || len(lastMsgHeadBuf) == 0 {
		return header, nil
	}
	if err := proto.Unmarshal(lastMsgHeadBuf, header); err != nil {
		return nil, nil
	}
	return header, nil
}

func getRawBlockInfoFromDB(db DB) (blockNum int64, blockHash string, err error) {
	data, _ := db.Get([]byte("LastBlock"))
	if len(data) == 0 {
		return -1, "", fmt.Errorf("LastBlock not found")
	}
	hash := utils.BytesToHash(data)
	data, _ = db.Get(append([]byte("H"), hash[:]...))
	if len(data) != 8 {
		return -1, "", fmt.Errorf("block number not found")
	}
	number := binary.BigEndian.Uint64(data)
	return int64(number), utils.HexEncode(hash[:]), nil
}

// GetDBInfo returns all opened DBs.
func (p *DBPool) GetDBInfo() (info *pb.DBInfoList, err error) {
	p.RLock()
	defer p.RUnlock()
	if p.metaDBID == math.MinInt32 {
		return nil, utils.ErrNoMetaDBRegistered
	}
	info = &pb.DBInfoList{}
	for id, db := range p.dbs {
		info.DbInfos = append(info.DbInfos, &pb.DBInfo{
			Id:     id,
			DbType: db.dbType,
			DbPath: db.path,
			IsMeta: db.isMeta,
		})
	}
	return info, nil
}

// GetDB returns a DB by id.
func (p *DBPool) GetDB(id int32) (db DB, err error) {
	p.RLock()
	defer p.RUnlock()
	dbWrap, ok := p.dbs[id]
	if !ok {
		return nil, leveldb.ErrNotFound
	}
	return dbWrap.db, nil
}

// GetDBID returns a DB's id by path.
func (p *DBPool) GetDBID(path string) (id int32, err error) {
	p.RLock()
	defer p.RUnlock()
	for _, db := range p.dbs {
		if db.path == path {
			return db.id, nil
		}
	}
	return -1, leveldb.ErrNotFound
}

// Marshal marshals write batchs to bytes.
func (p *DBPool) Marshal(batchs []BatchWithID) (batchItems []*pb.Data, err error) {
	for _, item := range batchs {
		tmp := make([]byte, len(item.B.Dump()))
		copy(tmp, item.B.Dump())
		batchItems = append(batchItems, &pb.Data{
			Id:   int32(item.ID),
			Data: tmp,
		})
	}
	return batchItems, nil
}

// WriteBlockInfo writes header Info to metaDB.
func (p *DBPool) WriteBlockInfo(header *pb.BlockInfo) (err error) {
	p.RLock()
	defer p.RUnlock()
	if p.metaDBID == math.MinInt32 {
		return utils.ErrNoMetaDBRegistered
	}
	lastMsgHeadBuf, err := proto.Marshal(header)
	if err != nil {
		return err
	}
	metaBatch := p.dbs[p.metaDBID].db.NewBatch()
	err = metaBatch.Put([]byte(LastBlockInfo), lastMsgHeadBuf)
	if err != nil {
		return err
	}
	err = metaBatch.Write()
	if err != nil {
		return err
	}
	return nil
}

// WriteBatchs writes batchs to DBs.
func (p *DBPool) WriteBatchs(batchs []BatchWithID) (err error) {
	for _, item := range batchs {
		err = item.B.Write()
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteBatchItems writes batchItems to DBs.
func (p *DBPool) WriteBatchItems(items []*pb.Data) (err error) {
	p.RLock()
	defer p.RUnlock()
	for _, item := range items {
		db := p.dbs[item.Id]
		batch := db.db.NewBatch()
		if err := batch.Load(item.Data); err != nil {
			return err
		}
		if err := batch.Write(); err != nil {
			return err
		}
	}
	return nil
}

func (p *DBPool) Close() {
	p.Lock()
	defer p.Unlock()
	for _, db := range p.dbs {
		db.db.Close()
	}
}
