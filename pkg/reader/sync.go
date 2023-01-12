package reader

import (
	"sync"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

const MaxChannelSize = 512

type broker struct {
	sync.Mutex
	subs          map[chan *pb.Block]struct{}
	lastMsgOffset int64
}

func newBroker(lastMsgOffset int64) *broker {
	return &broker{
		subs:          make(map[chan *pb.Block]struct{}),
		lastMsgOffset: lastMsgOffset,
	}
}

func (b *broker) subscribe() (chan *pb.Block, int64) {
	b.Lock()
	defer b.Unlock()
	msgCh := make(chan *pb.Block, MaxChannelSize)
	b.subs[msgCh] = struct{}{}
	return msgCh, b.lastMsgOffset
}

func (b *broker) unsubscribe(msgCh chan *pb.Block) {
	b.Lock()
	defer b.Unlock()
	delete(b.subs, msgCh)
	close(msgCh)
}

func (b *broker) publish(msg *pb.Block) {
	b.Lock()
	defer b.Unlock()
	b.lastMsgOffset = msg.Info.MsgOffset
	for msgCh := range b.subs {
		select {
		case msgCh <- msg:
		case <-time.After(2 * time.Second):
			close(msgCh)
			delete(b.subs, msgCh)
		}
	}
}

func (r *Reader) syncInit(bottomHash string) (memblocks []*pb.Block, ch chan *pb.Block, err error) {
	info := &pb.BlockInfo{
		ChainId:   r.config.ChainId,
		Env:       r.config.Env,
		BlockType: pb.BlockInfo_MEM,
		BlockRoot: bottomHash,
	}
	bottomBlock, err := r.s3.GetBlock(r.rootCtx, info, true)
	if err != nil {
		return nil, nil, err
	}
	utils.Logger().Info("Sync init", zap.Any("bottomBlock", bottomBlock))
	var startOffset int64
	if bottomBlock != nil {
		startOffset = bottomBlock.Info.MsgOffset + 1
	} else {
		startOffset = 0
	}
	ch, lastOffset := r.broker.subscribe()
	for startOffset <= lastOffset {
		infos, err := r.kafka.FetchStart(r.rootCtx, startOffset)
		if err != nil {
			return nil, nil, err
		}
		for _, info := range infos {
			info.BlockType = pb.BlockInfo_MEM
			block, err := r.s3.GetBlock(r.rootCtx, info, false)
			if err != nil {
				return nil, nil, err
			}
			block.Info = info
			memblocks = append(memblocks, block)
			startOffset += 1
		}
	}
	return memblocks, ch, nil
}

func (r *Reader) Sync(req *pb.SyncRequest, client pb.Remote_SyncServer) error {
	utils.Logger().Info("Sync", zap.Any("req", req))
	memblocks, ch, err := r.syncInit(req.BottomHash)
	defer r.broker.unsubscribe(ch)
	if err != nil {
		return status.Error(utils.BroadcasterErrorCode, err.Error())
	}
	for _, block := range memblocks {
		if err := client.Send(&pb.SyncyReply{
			Data: block,
		}); err != nil {
			utils.Logger().Error("Sync send", zap.Error(err))
			return status.Error(utils.BroadcasterErrorCode, err.Error())
		}
	}
	if len(memblocks) > 0 {
		utils.Logger().Info("Sync send memblocks sucess",
			zap.Any("from", memblocks[0].Info),
			zap.Any("to", memblocks[len(memblocks)-1].Info))
	}
	for {
		select {
		case <-r.rootCtx.Done():
			return nil
		case block := <-ch:
			utils.Logger().Info("Sync send new memblocks", zap.Any("block", block))
			if err := client.Send(&pb.SyncyReply{
				Data: block,
			}); err != nil {
				utils.Logger().Error("Sync send", zap.Error(err))
				return status.Error(utils.BroadcasterErrorCode, err.Error())
			}
		}
	}
}
