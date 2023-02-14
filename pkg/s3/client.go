package s3

import (
	"bytes"
	"context"
	"io"
	"math"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	s3client pb.S3ProxyClient
	cache    *utils.Cache
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
		cache:    utils.NewCache(MaxCacheSize),
	}, nil
}

func (c *Client) GetBlock(ctx context.Context, info *pb.BlockInfo, noCache bool) (header *pb.Block, err error) {
	commonPrefix := utils.CommonPrefix(info.Env, info.ChainId, info.Role, info.BlockType)
	lru := c.cache.GetOrCreatePrefixCache(commonPrefix)
	key := utils.InfoToPrefix(info)
	if noCache {
		val, err := c.getBlock(ctx, info, noCache)
		if err != nil {
			return nil, err
		}
		header = val
		lru.Insert(key, header, info.BlockNum)
	} else {
		val, err := lru.Get(key, info.BlockNum, func() (interface{}, error) { return c.getBlock(ctx, info, noCache) })
		if err != nil {
			return nil, err
		}
		header = proto.Clone(val.(*pb.Block)).(*pb.Block)
	}
	return header, nil
}

func (c *Client) getBlock(ctx context.Context, info *pb.BlockInfo, noCache bool) (header *pb.Block, err error) {
	client, err := c.s3client.GetBlock(ctx, &pb.GetBlockRequest{
		Info:    info,
		NoCache: noCache,
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
	if buf.Len() == 0 {
		return nil, nil
	}
	header = &pb.Block{}
	err = proto.Unmarshal(buf.Bytes(), header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (c *Client) PutBlock(ctx context.Context, block *pb.Block) (err error) {
	data, err := proto.Marshal(block)
	if err != nil {
		return err
	}
	utils.Logger().Info("put file", zap.Int("size", len(data)), zap.Any("info", block.Info))
	client, err := c.s3client.PutBlock(ctx)
	if err != nil {
		return err
	}
	block.Info.BlockSize = int64(len(data))
	err = client.Send(&pb.BlockChunk{
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
		if err := client.Send(&pb.BlockChunk{Chunk: chunk[:n]}); err != nil {
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

func (c *Client) PutFile(ctx context.Context, key string, buf []byte) error {
	_, err := c.s3client.PutFile(ctx, &pb.PutFileRequest{
		Path: key,
		Data: buf,
	})
	return err
}

func (c *Client) GetFile(ctx context.Context, key string) ([]byte, error) {
	rsp, err := c.s3client.GetFile(ctx, &pb.GetFileRequest{
		Path: key,
	})
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}
