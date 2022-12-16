package replicator

import (
	"context"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
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

	stop      bool
	stopC     chan struct{}
	stopdoneC chan struct{} // Write shutdown complete

}

// NewWriter creates a new writer.
func NewWriter(config *utils.Config, dbPool *db.DBPool) (writer *Writer, err error) {
	s3, err := s3.NewClient(config.S3ProxyAddr)
	if err != nil {
		return nil, err
	}

	topic := utils.Topic(config.ChainId, config.Env, true)

	kafka, err := kafka.NewKafkaClient(topic, -1, config.KafkaAddr)
	if err != nil {
		return nil, err
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}

	writer = &Writer{
		config:          config,
		dbPool:          dbPool,
		s3:              s3,
		kafka:           kafka,
		lastBlockHeader: lastBlockHeader,
		stopC:           make(chan struct{}),
		stopdoneC:       make(chan struct{}),
	}

	return writer, nil
}

// Recovery recovers the writer from the last block header.
func (w *Writer) Recovery() error {
	w.Lock()
	defer w.Unlock()
	info, err := w.s3.ListHeaderStartAt(context.Background(), w.config.ChainId,
		w.config.Env, w.lastBlockHeader.BlockNum, 100, w.lastBlockHeader.MsgOffset)
	if err != nil {
		return err
	}
	if len(info) > 0 {
		if len(info) != 1 {
			return utils.ErrWriterRecovey
		}
		w.lastBlockHeader = info[0]
		headerFile, err := w.s3.GetFile(context.Background(), w.lastBlockHeader)
		if err != nil {
			return err
		}
		blockFile, err := w.s3.GetFile(context.Background(), &pb.BlockInfo{
			ChainId:   w.config.ChainId,
			Env:       w.config.Env,
			BlockHash: headerFile.Info.BlockHash,
			BlockType: pb.BlockInfo_DATA,
		})
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
		err = w.WriteBlockHeaderToDB(make([]db.BatchWithID, 0))
		if err != nil {
			return err
		}
	}
	lastWriteOffset := w.kafka.LastWriterOffset()
	if lastWriteOffset != w.lastBlockHeader.MsgOffset {
		if lastWriteOffset != w.lastBlockHeader.MsgOffset-1 {
			return utils.ErrWriterRecovey
		}
		err = w.kafka.Broadcast(context.Background(), w.lastBlockHeader)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) WriteBlockToS3(blockNum int64, blockHash string, batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	batchItems, err := w.dbPool.Marshal(batchs)
	if err != nil {
		return err
	}
	Block := &pb.Block{
		Info: &pb.BlockInfo{
			ChainId:   w.config.ChainId,
			Env:       w.config.Env,
			BlockNum:  blockNum,
			BlockHash: blockHash,
			BlockType: pb.BlockInfo_DATA,
		},
		BatchItems: batchItems,
	}
	Block.Info.BlockSize = int64(proto.Size(Block))
	// commit to s3.
	err = w.s3.PutFile(context.Background(), Block)
	if err != nil {
		return err
	}
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

func (w *Writer) WriteBlockHeaderToS3(blockNum int64, blockHash string, batchs []db.BatchWithID) (err error) {
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
		Info: &pb.BlockInfo{
			ChainId:   w.config.ChainId,
			Env:       w.config.Env,
			BlockNum:  blockNum,
			BlockHash: blockHash,
			BlockType: pb.BlockInfo_HEADER,
			MsgOffset: w.lastBlockHeader.MsgOffset + 1,
		},
		BatchItems: batchItems,
	}
	// commit to s3.
	err = w.s3.PutFile(context.Background(), blockHeader)
	if err != nil {
		return err
	}
	w.lastBlockHeader = blockHeader.Info
	return nil
}

func (w *Writer) WriteBlockHeaderToDB(batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()
	if w.stop {
		return utils.ErrWriterStopped
	}
	err = w.dbPool.WriteBatchs(batchs)
	if err != nil {
		return err
	}
	err = w.dbPool.WriteBlockInfo(w.lastBlockHeader)
	if err != nil {
		return err
	}
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
	return nil
}
