package db

import (
	"math"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"google.golang.org/protobuf/proto"
)

const (
	LastBlockInfo = "rpl_last_bk"
)

// DBPool is a pool of Chain's All DBs.
type DBPool struct {
	sync.RWMutex
	dbs      map[int32]DB
	metaDBID int32
}

func NewDBPool() *DBPool {
	return &DBPool{
		dbs:      make(map[int32]DB),
		metaDBID: math.MinInt32,
	}
}

// RegisterMetaDB registers a DB.
// if isMetaDB is true, this DB will be used to store msgHead.
func (p *DBPool) Register(id int32, db DB, isMetaDB bool) {
	p.Lock()
	defer p.Unlock()
	p.dbs[id] = db
	if isMetaDB && p.metaDBID != math.MinInt32 {
		panic("meta db already registered")
	}
	if isMetaDB {
		p.metaDBID = id
	}
}

// GetBlockInfo returns the last BlockInfo from metaDB.
func (p *DBPool) GetBlockInfo() (header *pb.BlockInfo, err error) {
	p.RLock()
	defer p.RUnlock()
	if p.metaDBID == math.MinInt32 {
		return nil, utils.ErrNoMetaDBRegistered
	}
	db := p.dbs[p.metaDBID]
	lastMsgHeadBuf, err := db.Get([]byte(LastBlockInfo))
	if err != nil && err != utils.ErrNotFound {
		return nil, err
	}
	header = &pb.BlockInfo{
		BlockNum: -1,
	}
	if err == utils.ErrNotFound || len(lastMsgHeadBuf) == 0 {
		return header, nil
	}
	if err := proto.Unmarshal(lastMsgHeadBuf, header); err != nil {
		return nil, nil
	}
	return header, nil
}

func (p *DBPool) GetDB(id int32) (db DB, err error) {
	p.RLock()
	defer p.RUnlock()
	db, ok := p.dbs[id]
	if !ok {
		return nil, utils.ErrNotFound
	}
	return db, nil
}

func (p *DBPool) Marshal(batchs []BatchWithID) (batchItems []*pb.BatchItem, err error) {
	p.RLock()
	defer p.RUnlock()
	for _, item := range batchs {
		tmp := make([]byte, len(item.B.Dump()))
		copy(tmp, item.B.Dump())
		batchItems = append(batchItems, &pb.BatchItem{
			Id:   int32(item.ID),
			Data: tmp,
		})
	}
	return batchItems, nil
}

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
	metaBatch := p.dbs[p.metaDBID].NewBatch()
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

func (p *DBPool) WriteBatchs(batchs []BatchWithID) (err error) {
	p.RLock()
	defer p.RUnlock()
	for _, item := range batchs {
		err = item.B.Write()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *DBPool) WriteBatchItems(items []*pb.BatchItem) (err error) {
	p.RLock()
	defer p.RUnlock()
	for _, item := range items {
		db := p.dbs[item.Id]
		batch := db.NewBatch()
		if err := batch.Load(item.Data); err != nil {
			return err
		}
		if err := batch.Write(); err != nil {
			return err
		}
	}
	return nil
}
