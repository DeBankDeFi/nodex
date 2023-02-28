package ndrc

import (
	"context"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReaderClient struct {
	ndrcclient pb.SubscribeServiceClient
	conn       *grpc.ClientConn
	addr       string
}

func NewReaderClient(addr string) (*ReaderClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &ReaderClient{
		ndrcclient: pb.NewSubscribeServiceClient(conn),
		conn:       conn,
		addr:       addr,
	}, nil
}

func (rc *ReaderClient) WatchRole(ctx context.Context) <-chan string {
	ch := make(chan string)
	go rc.watchRole(ctx, ch)
	return ch
}

func (rc *ReaderClient) watchRole(ctx context.Context, ch chan<- string) {
	defer close(ch)
outer:
	for {
		time.Sleep(1 * time.Second)
		select {
		case <-ctx.Done():
			if rc.conn != nil {
				rc.conn.Close()
			}
			break outer
		default:
			if rc.conn != nil {
				if utils.CheckConnState(rc.conn) != nil {
					rc.conn.Close()
					conn, err := grpc.Dial(rc.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err != nil {
						continue
					}
					rc.ndrcclient = pb.NewSubscribeServiceClient(conn)
				}
			}
			stream, err := rc.ndrcclient.WatchWriterEvent(ctx, &pb.WriterEventSubcribeRequest{})
			if err != nil {
				continue
			}
		inner:
			for {
				resp, err := stream.Recv()
				if err != nil {
					break inner
				}
				if resp.Event == pb.WriterEvent_ROLE_CHANGED {
					if resp.Leader != nil {
						if resp.Leader.Role == pb.NodeRole_WRITERM {
							ch <- "master"
						} else if resp.Leader.Role == pb.NodeRole_WRITERB {
							ch <- "backup"
						}
					}
				}
			}
		}
	}
}
