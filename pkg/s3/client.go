package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	s3client pb.S3ProxyClient
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	return &Client{
		s3client: pb.NewS3ProxyClient(conn),
	}, nil
}

func (c *Client) GetFile(ctx context.Context, info *pb.BlockInfo) (header *pb.Block, err error) {
	client, err := c.s3client.GetFile(ctx, &pb.GetFileRequest{
		Info: info,
	})
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	for {
		chunk, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if len(chunk.Chunk) > 0 {
			buf.Write(chunk.Chunk)
		}
	}
	client.CloseSend()
	header = &pb.Block{}
	err = proto.Unmarshal(buf.Bytes(), header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (c *Client) PutFile(ctx context.Context, block *pb.Block) (err error) {
	data, err := proto.Marshal(block)
	if err != nil {
		return err
	}
	err = os.WriteFile(fmt.Sprintf("/tmp/wal/%d-%d",block.Info.BlockNum,block.Info.BlockType), data, 0644)
	if err != nil {
		return err
	}
	utils.Logger().Info("put file", zap.Int("size", len(data)), zap.Any("info", block.Info))
	client, err := c.s3client.PutFile(ctx)
	if err != nil {
		return err
	}
	block.Info.BlockSize = int64(len(data))
	err = client.Send(&pb.FileChunk{
		Info: block.Info,
	})
	if err != nil {
		return err
	}
	chunk := make([]byte, ChunkSize)
	reader := bytes.NewReader(data)
	for {
		n, err := reader.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := client.Send(&pb.FileChunk{Chunk: chunk[:n]}); err != nil {
			return err
		}
	}
	_, err = client.CloseAndRecv()
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) ListHeaderStartAt(ctx context.Context, chainId, env string, blockNum int64, count int64, after int64) (info []*pb.BlockInfo, err error) {
	rsp, err := c.s3client.ListHeaderStartAt(ctx, &pb.ListHeaderStartAtRequest{
		ChainId:        chainId,
		Env:            env,
		BlockNum:       blockNum,
		CountNum:       count,
		AfterMsgOffset: after,
	})
	if err != nil {
		return nil, err
	}
	return rsp.Infos, nil
}

func (c *Client) RemoveFiles(ctx context.Context, infos []*pb.BlockInfo) error {
	_, err := c.s3client.RemoveFiles(ctx, &pb.RemoveFilesRequest{
		Infos: infos,
	})
	return err
}
