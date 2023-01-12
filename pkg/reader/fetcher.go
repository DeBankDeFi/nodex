package reader

import (
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
)

func (r *Reader) fetchAndCommit() error {
	infos, err := r.kafka.Fetch(r.rootCtx)
	if err != nil {
		utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
		return err
	}
	for _, info := range infos {
		utils.Logger().Info("new header", zap.Any("BlockNum", info.BlockNum), zap.Any("MsgOffset", info.MsgOffset))
		headerFile, err := r.s3.GetBlock(r.rootCtx, info, true)
		if err != nil {
			utils.Logger().Error("GetHeaderFile error", zap.Error(err), zap.Any("info", info))
			return err
		}
		info.BlockType = pb.BlockInfo_DATA
		blockFile, err := r.s3.GetBlock(r.rootCtx, info, true)
		if err != nil {
			utils.Logger().Error("GetBlockFile error", zap.Error(err), zap.Any("hash", headerFile.Info.BlockHash))
			return err
		}
		info.BlockType = pb.BlockInfo_MEM
		memFile, err := r.s3.GetBlock(r.rootCtx, info, true)
		if err != nil {
			utils.Logger().Error("GetMemFile error", zap.Error(err), zap.Any("hash", headerFile.Info.BlockHash))
			return err
		}
		err = r.dbPool.WriteBatchItems(blockFile.BatchItems)
		if err != nil {
			utils.Logger().Error("WriteBatchItems error", zap.Error(err))
			return err
		}
		err = r.dbPool.WriteBatchItems(headerFile.BatchItems)
		if err != nil {
			utils.Logger().Error("WriteBatchItems error", zap.Error(err))
			return err
		}
		err = r.dbPool.WriteBlockInfo(info)
		if err != nil {
			utils.Logger().Error("WriteBlockInfo error", zap.Error(err))
			return err
		}
		r.broker.publish(memFile)
		r.lastBlockHeader = info
		if r.kafka.LastReaderOffset()+1 != r.lastBlockHeader.MsgOffset {
			utils.Logger().Error("LastReaderOffset error", zap.Any("kafka", r.kafka.LastReaderOffset()), zap.Any("block", r.lastBlockHeader.MsgOffset))
		}
		r.kafka.IncrementLastReaderOffset()
		utils.Logger().Info("Apply Block success", zap.Any("blockInfo", r.lastBlockHeader.String()))
	}
	return nil
}

func (r *Reader) reset(config *utils.Config) error {
	topic := utils.Topic(config.ChainId, config.Env)
	r.kafka.ResetTopic(topic)
	infos, err := r.s3.ListHeaderStartAt(r.rootCtx, config.ChainId, config.Env,
		r.lastBlockHeader.BlockNum-1, 5, r.lastBlockHeader.MsgOffset)
	if err != nil {
		utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
		return err
	}
	for _, info := range infos {
		if info.BlockHash == r.lastBlockHeader.BlockHash {
			r.lastBlockHeader = info
			r.config = config
			return nil
		}
	}
	infos, err = r.s3.ListHeaderStartAt(r.rootCtx, config.ChainId, config.Env,
		r.lastBlockHeader.BlockNum-128, 5, -1)
	if err != nil {
		utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
		return err
	}
	r.lastBlockHeader = infos[0]
	r.config = config
	return nil
}

func (r *Reader) fetchRun() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			err := r.fetchAndCommit()
			if err != nil {
				utils.Logger().Error("fetchAndCommit error", zap.Error(err))
			}
		case config := <-r.resetC:
			err := r.reset(config)
			if err != nil {
				utils.Logger().Error("reset error", zap.Error(err))
			}
		case <-r.rootCtx.Done():
			r.stopdoneC <- struct{}{}
			return
		}
	}
}
