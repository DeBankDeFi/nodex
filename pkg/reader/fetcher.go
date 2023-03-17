package reader

import (
	"runtime"
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
	for i, info := range infos {
		if i%100 == 0 {
			runtime.GC()
		}
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
		if blockFile != nil {
			err = r.dbPool.WriteBatchItems(blockFile.BatchItems)
			if err != nil {
				utils.Logger().Error("WriteBatchItems error", zap.Error(err))
				return err
			}
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
		headerFile.BatchItems = nil
		r.broker.publish(headerFile)
		r.lastBlockHeader = info
		if r.kafka.LastReaderOffset()+1 != r.lastBlockHeader.MsgOffset {
			utils.Logger().Error("LastReaderOffset error", zap.Any("kafka", r.kafka.LastReaderOffset()), zap.Any("block", r.lastBlockHeader.MsgOffset))
		}
		r.kafka.IncrementLastReaderOffset()
		utils.Logger().Info("Apply Block success", zap.Any("blockInfo", r.lastBlockHeader.String()))
	}
	return nil
}

func (r *Reader) reset(role string) error {
	topic := utils.Topic(r.config.Env, r.config.ChainId, role)
	r.kafka.ResetTopic(topic)
	infos, err := r.s3.ListHeaderStartAt(r.rootCtx, r.config.ChainId, r.config.Env, role,
		r.lastBlockHeader.BlockNum-1, 5, -1)
	if err != nil {
		utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
		return err
	}
	for _, info := range infos {
		if info.BlockHash == r.lastBlockHeader.BlockHash {
			r.lastBlockHeader = info
			r.config.Role = role
			r.kafka.ResetLastReaderOffset(info.MsgOffset)
			return nil
		}
	}
	infos, err = r.s3.ListHeaderStartAt(r.rootCtx, r.config.ChainId, r.config.Env, role,
		r.lastBlockHeader.BlockNum-128, 5, -1)
	if err != nil {
		utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
		return err
	}
	r.lastBlockHeader = infos[0]
	r.config.Role = role
	r.kafka.ResetLastReaderOffset(r.lastBlockHeader.MsgOffset)
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
		case role := <-r.resetC:
			utils.Logger().Info("reset", zap.Any("new role", role), zap.Any("old role", r.config.Role))
			if role == r.config.Role || role == "" {
				continue
			}
			err := r.reset(role)
			if err != nil {
				utils.Logger().Error("reset error", zap.Error(err))
			}
		case <-r.rootCtx.Done():
			r.stopdoneC <- struct{}{}
			return
		}
	}
}
