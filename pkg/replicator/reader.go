package replicator

import (
	"context"
	"sync"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/blockpb"
	"go.uber.org/zap"
)

type Reader struct {
	sync.Mutex

	dbPool *db.DBPool
	s3     *Client

	lastBlockHeader *blockpb.BlockInfo
	reorgDeep       int32
	resetChan       <-chan *utils.Config

	running   bool
	stopC     chan struct{}
	stopdoneC chan struct{} // Reader shutdown complete
	logger    *zap.Logger
}

func NewReader(config *utils.Config, dbPool *db.DBPool, resetChan <-chan *utils.Config) (reader *Reader, err error) {
	s3, err := NewS3Client(context.Background(), config.Bucket, config.EnvPrex)
	if err != nil {
		return nil, err
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}

	reader = &Reader{
		dbPool:          dbPool,
		s3:              s3,
		lastBlockHeader: lastBlockHeader,
		reorgDeep:       config.ReorgDeep,
		stopC:           make(chan struct{}),
		stopdoneC:       make(chan struct{}),
		logger:          utils.Logger(),
	}
	return reader, nil
}

func (r *Reader) Start() (err error) {
	go r.run()
	return
}

func (r *Reader) Stop() {
	r.Lock()
	defer r.Unlock()
	if !r.running {
		return
	}
	r.running = false
	close(r.stopC)
	<-r.stopdoneC
}

func (r *Reader) run() {
	defer r.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			objInfos, err := r.s3.ListHeaderStartAt(context.Background(), r.lastBlockHeader.BlockNum-int64(r.reorgDeep), r.reorgDeep*2, r.lastBlockHeader.LastTime)
			if err != nil {
				r.logger.Error("ListHeaderStartAt error", zap.Error(err))
				continue
			}
			for _, objInfo := range objInfos {
				headerFile, err := r.s3.GetHeaderFile(context.Background(), objInfo.Key)
				if err != nil {
					r.logger.Error("GetHeaderFile error", zap.Error(err), zap.Any("key", objInfo.Key))
					break
				}
				blockFile, err := r.s3.GetBlockFile(context.Background(), headerFile.Info.BlockHash)
				if err != nil {
					r.logger.Error("GetBlockFile error", zap.Error(err), zap.Any("hash", headerFile.Info.BlockHash))
					break
				}
				err = r.dbPool.WriteBatchItems(blockFile.BatchItems)
				if err != nil {
					r.logger.Error("WriteBatchItems error", zap.Error(err))
					break
				}
				err = r.dbPool.WriteBatchItems(headerFile.BatchItems)
				if err != nil {
					r.logger.Error("WriteBatchItems error", zap.Error(err))
					break
				}
				if headerFile.Info.BlockNum > r.lastBlockHeader.BlockNum {
					r.lastBlockHeader.BlockNum = headerFile.Info.BlockNum
					r.lastBlockHeader.BlockHash = headerFile.Info.BlockHash
				}
				if headerFile.Info.LastTime > r.lastBlockHeader.LastTime {
					r.lastBlockHeader.LastTime = headerFile.Info.LastTime
				}
				err = r.dbPool.WriteBlockInfo(r.lastBlockHeader)
				if err != nil {
					r.logger.Error("WriteBlockInfo error", zap.Error(err))
					break
				}
				r.logger.Info("WriteBlockInfo success", zap.Any("blockInfo", r.lastBlockHeader.String()))
			}
		case config := <-r.resetChan:
			r.logger.Info("resetChan", zap.Any("config", config))
			s3, err := NewS3Client(context.Background(), config.Bucket, config.EnvPrex)
			if err != nil {
				r.logger.Error("NewS3Client error", zap.Error(err))
				continue
			}
			r.s3 = s3
			r.lastBlockHeader.LastTime = 0
		case <-r.stopC:
			r.stopdoneC <- struct{}{}
			return
		}
	}
}
