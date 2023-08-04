package reader

import (
	"context"

	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/utils"
)

type SyncClient struct {
	client pb.RemoteClient
	stream pb.Remote_SyncClient
	fn     context.CancelFunc
	ctx    context.Context
}

func NewSyncClient(client pb.RemoteClient) *SyncClient {
	ctx, fn := context.WithCancel(context.Background())
	return &SyncClient{
		client: client,
		ctx:    ctx,
		fn:     fn,
	}
}

func (r *SyncClient) SyncInit() error {
	stream, err := r.client.Sync(r.ctx, &pb.SyncRequest{})
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
