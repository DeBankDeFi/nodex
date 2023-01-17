package reader

import (
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

const MaxChannelSize = 512

type broker struct {
	sync.Mutex
	subs map[chan *pb.Block]struct{}
}

func newBroker(lastMsgOffset int64) *broker {
	return &broker{
		subs: make(map[chan *pb.Block]struct{}),
	}
}

func (b *broker) subscribe() chan *pb.Block {
	b.Lock()
	defer b.Unlock()
	msgCh := make(chan *pb.Block, MaxChannelSize)
	b.subs[msgCh] = struct{}{}
	return msgCh
}

func (b *broker) unsubscribe(msgCh chan *pb.Block) {
	b.Lock()
	defer b.Unlock()
	if _, ok := b.subs[msgCh]; ok {
		delete(b.subs, msgCh)
		close(msgCh)
	}
}

func (b *broker) publish(msg *pb.Block) {
	b.Lock()
	defer b.Unlock()
	for msgCh := range b.subs {
		select {
		case msgCh <- msg:
		default:
			if _, ok := b.subs[msgCh]; ok {
				delete(b.subs, msgCh)
				close(msgCh)
			}
		}
	}
}

func (r *Reader) Sync(req *pb.SyncRequest, client pb.Remote_SyncServer) error {
	utils.Logger().Info("Sync", zap.Any("req", req))
	ch := r.broker.subscribe()
	defer r.broker.unsubscribe(ch)
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
