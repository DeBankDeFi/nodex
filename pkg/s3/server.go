package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	BucketName   = "blockchain-replicator"
	MaxCacheSize = 256
	ChunkSize    = 1 << 22
)

type server struct {
	pb.UnimplementedS3ProxyServer
	s3    *s3.Client
	cache *utils.Cache
	sync.RWMutex
	getPool sync.Pool
}

func ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv, err := NewServer()
	if err != nil {
		return err
	}
	return srv.Serve(ln)
}

func NewServer() (*grpc.Server, error) {
	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	s3 := s3.NewFromConfig(sdkConfig)
	s := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32))
	pb.RegisterS3ProxyServer(s, &server{
		cache: utils.NewCache(MaxCacheSize),
		s3:    s3,
		getPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, ChunkSize)
				return &buf
			},
		},
	})
	return s, nil
}

func (s *server) s3GetFile(key string) (buf []byte, err error) {
	result, err := s.s3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})
	if result != nil {
		defer result.Body.Close()
	}
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, nil
		}
		return nil, err
	}
	buf, err = io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *server) GetBlock(req *pb.GetBlockRequest, client pb.S3Proxy_GetBlockServer) error {
	commonPrefix := utils.CommonPrefix(req.Info.Env, req.Info.ChainId, req.Info.Role, req.Info.BlockType)
	lru := s.cache.GetOrCreatePrefixCache(commonPrefix)
	key := utils.InfoToPrefix(req.Info)
	var buf []byte
	if req.NoCache {
		val, err := s.s3GetFile(key)
		if err != nil {
			return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
		}
		buf = val
	} else {
		val, err := lru.Get(key, req.Info.BlockNum, func() (interface{}, error) { return s.s3GetFile(key) })
		if err != nil {
			return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
		}
		buf = val.([]byte)
	}
	chunk := s.getPool.Get().(*[]byte)
	defer s.getPool.Put(chunk)
	reader := bytes.NewReader(buf)
	for {
		n, err := reader.Read(*chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
		}
		if err := client.Send(&pb.BlockChunk{Chunk: (*chunk)[:n]}); err != nil {
			return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
		}
	}
	return nil
}

func (s *server) PutBlock(client pb.S3Proxy_PutBlockServer) error {
	chunk, err := client.Recv()
	if err != nil {
		return status.Errorf(utils.AwsS3ErrorCode, "PutBlock failed, err : %v", err)
	}
	buf := make([]byte, int(chunk.Info.BlockSize))
	buf = buf[:0]

	buffer := bytes.NewBuffer(buf)
	info := chunk.Info
	for {
		chunk, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(utils.AwsS3ErrorCode, "PutBlock failed, err : %v", err)
		}
		if len(chunk.Chunk) > 0 {
			buffer.Write(chunk.Chunk)
		}
	}
	commonPrefix := utils.CommonPrefix(info.Env, info.ChainId, info.Role, info.BlockType)
	key := utils.InfoToPrefix(info)
	rsp, err := s.s3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		return status.Errorf(utils.AwsS3ErrorCode, "PutBlock failed, err : %v", err)
	}
	utils.Logger().Info("PutBlock", zap.String("key", key), zap.Any("rsp", rsp))
	lru := s.cache.GetOrCreatePrefixCache(commonPrefix)
	lru.Insert(key, buffer.Bytes(), info.BlockNum)
	client.SendAndClose(&pb.PutBlockReply{})
	return nil
}

func (s *server) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileReply, error) {
	buf, err := s.s3GetFile(req.Path)
	if err != nil {
		return nil, status.Errorf(utils.AwsS3ErrorCode, "GetFile failed, err : %v", err)
	}
	return &pb.GetFileReply{Data: buf}, nil
}

func (s *server) PutFile(ctx context.Context, req *pb.PutFileRequest) (*pb.PutFileReply, error) {
	_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(req.Path),
		Body:   bytes.NewReader(req.Data),
	})
	if err != nil {
		return nil, status.Errorf(utils.AwsS3ErrorCode, "PutFile failed, err : %v", err)
	}
	utils.Logger().Info("PutFile", zap.String("key", req.Path), zap.Any("err", err))
	return &pb.PutFileReply{}, nil
}

func (s *server) ListHeaderStartAt(ctx context.Context, req *pb.ListHeaderStartAtRequest) (*pb.ListHeaderStartAtReply, error) {
	prefix := utils.CommonPrefix(req.Env, req.ChainId, req.Role, pb.BlockInfo_HEADER)
	result, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(BucketName),
		Prefix:     aws.String(prefix),
		MaxKeys:    int32(req.CountNum),
		StartAfter: aws.String(fmt.Sprintf("%s/%012d", prefix, req.BlockNum)),
	})
	if err != nil {
		return nil, err
	}
	rsp := &pb.ListHeaderStartAtReply{}
	for _, object := range result.Contents {
		info, err := utils.PrefixToHeaderInfo(*object.Key)
		if err != nil {
			utils.Logger().Error("ListHeaderStartAt failed", zap.String("key", *object.Key))
			return nil, status.Errorf(utils.ReadInvalidHeaderErrorCode, "ListHeaderStartAt failed, key : %s", *object.Key)
		}
		if int64(info.MsgOffset) > req.AfterMsgOffset {
			rsp.Infos = append(rsp.Infos, info)
		}
	}
	return rsp, nil
}

func (s *server) RemoveFiles(ctx context.Context, req *pb.RemoveFilesRequest) (rsp *pb.RemoveFilesReply, err error) {
	for _, info := range req.Infos {
		key := ""
		if info.BlockType == pb.BlockInfo_DATA {
			key = utils.BlockPrefix(info)
		} else {
			key = utils.HeaderPrefix(info)
		}
		_, err = s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(BucketName),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, err
		}
	}
	return &pb.RemoveFilesReply{}, nil

}
