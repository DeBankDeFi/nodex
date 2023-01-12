package reader

import (
	"context"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Writer represents a writer that appends db's write batch to the wal
// and broadcasts to the reader by message queue.
type Writer struct {
	sync.Mutex

	config *utils.Config

	dbPool *db.DBPool
	s3     *s3.Client
	kafka  *kafka.KafkaClient

	lastBlockHeader *pb.BlockInfo

	stop bool
}

// NewWriter creates a new writer.
func NewWriter(config *utils.Config, dbPool *db.DBPool) (writer *Writer, err error) {
	s3, err := s3.NewClient(config.S3ProxyAddr)
	if err != nil {
		return nil, err
	}

	topic := utils.Topic(config.ChainId, config.Env)

	kafka, err := kafka.NewKafkaClient(topic, -1, config.KafkaAddr)
	if err != nil {
		return nil, err
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}

	utils.Logger().Info("NewWriter", zap.Any("lastBlockHeader", lastBlockHeader))

	writer = &Writer{
		config:          config,
		dbPool:          dbPool,
		s3:              s3,
		kafka:           kafka,
		lastBlockHeader: lastBlockHeader,
	}

	return writer, nil
}

// Recovery recovers the writer from the last block header.
func (w *Writer) Recovery() error {
	w.Lock()
	defer w.Unlock()
	startWriteOffset := w.lastBlockHeader.MsgOffset + 1
	lastWriteOffset := w.kafka.LastWriterOffset()
	for startWriteOffset <= lastWriteOffset {
		infos, err := w.kafka.FetchStart(context.Background(), startWriteOffset)
		if err != nil {
			return err
		}
		if len(infos) > 0 {
			if len(infos) != 1 {
				return utils.ErrWriterRecovey
			}
			info := infos[0]
			utils.Logger().Info("Recovery", zap.Any("lastBlockHeader", w.lastBlockHeader), zap.Any("info", info))
			w.lastBlockHeader = info
			headerFile, err := w.s3.GetBlock(context.Background(), w.lastBlockHeader, false)
			if err != nil {
				return err
			}
			blockFile, err := w.s3.GetBlock(context.Background(), &pb.BlockInfo{
				ChainId:   w.config.ChainId,
				Env:       w.config.Env,
				BlockHash: headerFile.Info.BlockHash,
				BlockType: pb.BlockInfo_DATA,
			}, false)
			if err != nil {
				return err
			}
			err = w.dbPool.WriteBatchItems(headerFile.BatchItems)
			if err != nil {
				return err
			}
			err = w.dbPool.WriteBatchItems(blockFile.BatchItems)
			if err != nil {
				return err
			}
			w.lastBlockHeader = headerFile.Info
			err = w.WriteBlockHeaderToDB(w.lastBlockHeader, make([]db.BatchWithID, 0))
			if err != nil {
				return err
			}
		}
	}

	utils.Logger().Info("Recovery", zap.Int64("lastWriteOffset", lastWriteOffset),
		zap.Int64("lastBlockHeader.MsgOffset", w.lastBlockHeader.MsgOffset))
	if lastWriteOffset != w.lastBlockHeader.MsgOffset {
		if lastWriteOffset != w.lastBlockHeader.MsgOffset-1 {
			return utils.ErrWriterRecovey
		}
		err := w.kafka.Broadcast(context.Background(), w.lastBlockHeader)
		if err != nil {
			return err
		}
	}
	dbInfos, err := w.dbPool.GetDBInfo()
	if err != nil {
		utils.Logger().Error("GetDBInfo", zap.Error(err))
		return err
	}
	utils.Logger().Info("Recovery", zap.Any("dbInfos", dbInfos))
	buf, err := proto.Marshal(dbInfos)
	if err != nil {
		utils.Logger().Error("Marshal", zap.Error(err))
		return err
	}

	err = w.s3.PutFile(context.Background(), utils.DBInfoPrefix(w.config.ChainId, w.config.Env), buf)
	if err != nil {
		utils.Logger().Error("PutFile", zap.Error(err))
		return err
	}
	utils.Logger().Info("Recovery sucess", zap.Any("lastBlockHeader", w.lastBlockHeader))
	return nil
}

func (w *Writer) PrepareBlockInfo(blockNum int64, blockHash string, blockRoot string) *pb.BlockInfo {
	return &pb.BlockInfo{
		ChainId:   w.config.ChainId,
		Env:       w.config.Env,
		BlockNum:  blockNum,
		BlockHash: blockHash,
		BlockRoot: blockRoot,
		MsgOffset: w.lastBlockHeader.MsgOffset + 1,
	}
}

func (w *Writer) WriteBlockToS3(info *pb.BlockInfo, batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	batchItems, err := w.dbPool.Marshal(batchs)
	if err != nil {
		return err
	}
	block := &pb.Block{
		Info:       info,
		BatchItems: batchItems,
	}
	block.Info.BlockType = pb.BlockInfo_DATA
	block.Info.BlockSize = int64(proto.Size(block))
	// commit to s3.
	err = w.s3.PutBlock(context.Background(), block)
	if err != nil {
		return err
	}
	utils.Logger().Debug("WriteBlockToS3", zap.Any("Block.Info", block.Info))
	return nil
}

func (w *Writer) WriteBlockToDB(batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	err = w.dbPool.WriteBatchs(batchs)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteBlockHeaderToS3(info *pb.BlockInfo, batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	batchItems, err := w.dbPool.Marshal(batchs)
	if err != nil {
		return err
	}
	blockHeader := &pb.Block{
		Info:       info,
		BatchItems: batchItems,
	}
	blockHeader.Info.BlockType = pb.BlockInfo_HEADER
	// commit to s3.
	err = w.s3.PutBlock(context.Background(), blockHeader)
	if err != nil {
		return err
	}
	utils.Logger().Debug("WriteBlockHeaderToS3", zap.Any("blockHeader.Info", blockHeader.Info))
	return nil
}

func (w *Writer) WriteBlockMemDiffToS3(info *pb.BlockInfo, snapshotDiff *pb.SnapshotDiff, cacheDiff []*pb.KV) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	memDiff := &pb.Block{
		Info:         info,
		SnapshotDiff: snapshotDiff,
		AddedCache:   cacheDiff,
	}
	memDiff.Info.BlockType = pb.BlockInfo_MEM
	// commit to s3.
	err = w.s3.PutBlock(context.Background(), memDiff)
	if err != nil {
		return err
	}
	utils.Logger().Debug("WriteBlockMemDiffToS3", zap.Any("memDiff.Info", memDiff.Info))
	return nil
}

func (w *Writer) WriteBlockHeaderToDB(info *pb.BlockInfo, batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	err = w.dbPool.WriteBatchs(batchs)
	if err != nil {
		return err
	}
	err = w.dbPool.WriteBlockInfo(info)
	if err != nil {
		return err
	}
	w.lastBlockHeader = info
	return nil
}

func (w *Writer) WriteBlockHeaderToKafka() (err error) {
	w.Lock()
	defer w.Unlock()

	if w.stop {
		return utils.ErrWriterStopped
	}
	err = w.kafka.Broadcast(context.Background(), w.lastBlockHeader)
	if err != nil {
		return err
	}
	utils.Logger().Info("WriteBlockHeaderToKafka sucess", zap.Any("lastBlockHeader", w.lastBlockHeader))
	return nil
}