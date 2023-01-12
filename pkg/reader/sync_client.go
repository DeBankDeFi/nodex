package reader

import (
	"context"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
)

type SyncClient struct {
	client pb.RemoteClient
	stream pb.Remote_SyncClient
	fn     context.CancelFunc
}

func NewSyncClient(client pb.RemoteClient) *SyncClient {
	return &SyncClient{
		client: client,
	}
}

func (r *SyncClient) SyncInit(bottomHash string) error {
	ctx, fn := context.WithCancel(context.Background())
	r.fn = fn
	stream, err := r.client.Sync(ctx, &pb.SyncRequest{
		BottomHash: bottomHash,
	})
	if err != nil {
		return err
	}
	r.stream = stream
	return nil
}

func (r *SyncClient) SyncNext() (*pb.Block, error) {
	if r.stream == nil {
		return nil, utils.ErrStreamNotInit
	}
	msg, err := r.stream.Recv()
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (r *SyncClient) Cancel() {
	if r.fn != nil {
		r.fn()
	}
}
