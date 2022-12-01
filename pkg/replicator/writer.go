package replicator

import (
	"context"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/blockpb"
	"go.uber.org/zap"
)

// Writer represents a writer that appends db's write batch to the wal
// and broadcasts to the reader by message queue.
type Writer struct {
	sync.Mutex

	dbPool *db.DBPool
	s3     *Client

	lastBlockHeader *blockpb.BlockInfo

	stop      bool
	stopC     chan struct{}
	stopdoneC chan struct{} // Write shutdown complete

	logger *zap.Logger
}

// NewWriter creates a new writer.
func NewWriter(config *utils.Config, dbPool *db.DBPool) (writer *Writer, err error) {
	s3, err := NewS3Client(context.Background(), config.Bucket, config.EnvPrex)
	if err != nil {
		return nil, err
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}

	writer = &Writer{
		dbPool:          dbPool,
		s3:              s3,
		lastBlockHeader: lastBlockHeader,
		stopC:           make(chan struct{}),
		stopdoneC:       make(chan struct{}),
		logger:          utils.Logger(),
	}

	return writer, nil
}

func (w *Writer) WriteBlockDataToS3(blockNum int64, blockHash string, batchs []db.BatchWithID) (err error) {
	w.Lock()
	defer w.Unlock()

	if w.stop {
		return utils.ErrWriterStopped
	}
	batchItems, err := w.dbPool.Marshal(batchs)
	if err != nil {
		return err
	}
	blockData := &blockpb.BlockData{
		Info: &blockpb.BlockInfo{
			BlockNum:  blockNum,
			BlockHash: blockHash,
		},
		BatchItems: batchItems,
	}
	// commit to s3.
	err = w.s3.PutBlockFile(context.Background(), blockData)
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
	blockHeader := &blockpb.BlockHeader{
		Info: &blockpb.BlockInfo{
			BlockNum:  blockNum,
			BlockHash: blockHash,
		},
		BatchItems: batchItems,
	}
	// commit to s3.
	err = w.s3.PutHeaderFile(context.Background(), blockHeader)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) WriteBlockHeaderToDB() (err error) {
	w.Lock()
	defer w.Unlock()

	if w.stop {
		return utils.ErrWriterStopped
	}
	err = w.dbPool.WriteBlockInfo(w.lastBlockHeader)
	if err != nil {
		return err
	}
	return nil
}
