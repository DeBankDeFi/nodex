package replicator

import (
	"context"
	"sync"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	sync.Mutex

	config *utils.Config

	dbPool *db.DBPool
	s3     *s3.Client
	kafka  *kafka.KafkaClient

	lastBlockHeader *pb.BlockInfo
	reorgDeep       int
	resetChan       <-chan *utils.Config

	running   bool
	stopC     chan struct{}
	stopdoneC chan struct{} // Reader shutdown complete
}

func NewReader(config *utils.Config, dbPool *db.DBPool, resetChan <-chan string) (reader *Reader, err error) {
	s3, err := s3.NewClient(config.S3ProxyAddr)
	if err != nil {
		return nil, err
	}

	buf, err := s3.GetFile(context.Background(), utils.DBInfoPrefix(config.ChainId, config.Env))
	if err != nil {
		return nil, err
	}

	dbInfos := &pb.DBInfoList{}
	if err := proto.Unmarshal(buf, dbInfos); err != nil {
		return nil, err
	}

	utils.Logger().Info("NewReader", zap.Any("dbInfos", dbInfos))

	for _, dbInfo := range dbInfos.DbInfos {
		err = dbPool.Open(dbInfo)
		if err != nil {
			return nil, err
		}
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}
	utils.Logger().Info("NewReader", zap.Int64("block_num", lastBlockHeader.BlockNum), zap.Int64("msg_offset", lastBlockHeader.MsgOffset))

	topic := utils.Topic(config.ChainId, config.Env, true)

	kafka, err := kafka.NewKafkaClient(topic, lastBlockHeader.MsgOffset, config.KafkaAddr)
	if err != nil {
		return nil, err
	}

	reader = &Reader{
		config:          config,
		dbPool:          dbPool,
		s3:              s3,
		kafka:           kafka,
		lastBlockHeader: lastBlockHeader,
		reorgDeep:       config.ReorgDeep,
		stopC:           make(chan struct{}),
		stopdoneC:       make(chan struct{}),
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
	r.running = true
	defer r.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
Loop:
	for {
		select {
		case <-ticker.C:
			infos, err := r.kafka.Fetch(context.Background())
			if err != nil {
				utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
				continue
			}
			for _, info := range infos {
				utils.Logger().Info("new header", zap.Any("BlockNum", info.BlockNum), zap.Any("MsgOffset", r.lastBlockHeader.MsgOffset))
				headerFile, err := r.s3.GetBlock(context.Background(), info)
				if err != nil {
					utils.Logger().Error("GetHeaderFile error", zap.Error(err), zap.Any("info", info))
					break
				}
				info.BlockType = pb.BlockInfo_DATA
				blockFile, err := r.s3.GetBlock(context.Background(), info)
				if err != nil {
					utils.Logger().Error("GetBlockFile error", zap.Error(err), zap.Any("hash", headerFile.Info.BlockHash))
					break
				}
				err = r.dbPool.WriteBatchItems(blockFile.BatchItems)
				if err != nil {
					utils.Logger().Error("WriteBatchItems error", zap.Error(err))
					break
				}
				err = r.dbPool.WriteBatchItems(headerFile.BatchItems)
				if err != nil {
					utils.Logger().Error("WriteBatchItems error", zap.Error(err))
					break
				}
				r.lastBlockHeader = info
				r.lastBlockHeader.MsgOffset = r.kafka.LastReaderOffset()
				r.kafka.IncrementLastReaderOffset()
				err = r.dbPool.WriteBlockInfo(r.lastBlockHeader)
				if err != nil {
					utils.Logger().Error("WriteBlockInfo error", zap.Error(err))
					break
				}
				utils.Logger().Info("WriteBlockInfo success", zap.Any("blockInfo", r.lastBlockHeader.String()))
			}
		case config := <-r.resetChan:
			topic := utils.Topic(config.ChainId, config.Env, true)
			r.kafka.ResetTopic(topic)
			infos, err := r.s3.ListHeaderStartAt(context.Background(), config.ChainId, config.Env,
				r.lastBlockHeader.BlockNum-1, 5, r.lastBlockHeader.MsgOffset)
			if err != nil {
				utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
				continue Loop
			}
			for _, info := range infos {
				if info.BlockHash == r.lastBlockHeader.BlockHash {
					r.lastBlockHeader = info
					r.config = config
					continue Loop
				}
			}
			infos, err = r.s3.ListHeaderStartAt(context.Background(), config.ChainId, config.Env,
				r.lastBlockHeader.BlockNum-128, 5, -1)
			if err != nil {
				utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
				continue Loop
			}
			r.lastBlockHeader = infos[0]
			r.config = config

		case <-r.stopC:
			r.stopdoneC <- struct{}{}
			return
		}
	}
}
