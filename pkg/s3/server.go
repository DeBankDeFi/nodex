package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

const (
	BucketName = "blockchain-replicator"
	ChunkSize  = 1 << 22
)

type server struct {
	pb.UnimplementedS3ProxyServer
	s3  *s3.Client
	lru map[string]*utils.Cache
	sync.RWMutex
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
		lru: make(map[string]*utils.Cache),
		s3:  s3,
	})
	return s, nil
}

func (s *server) s3GetBlock(key string) (buf []byte, err error) {
	result, err := s.s3.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})
	if result != nil {
		defer result.Body.Close()
	}
	if err != nil {
		return nil, err
	}
	buf, err = io.ReadAll(result.Body)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (s *server) getOrInsertLru(prefix string) (lru *utils.Cache) {
	s.RLock()
	if lru, ok := s.lru[prefix]; ok {
		s.RUnlock()
		return lru
	}
	s.RUnlock()
	lru = utils.NewLru(128)
	s.Lock()
	if _, ok := s.lru[prefix]; !ok {
		s.lru[prefix] = lru
	} else {
		lru = s.lru[prefix]
	}
	s.Unlock()
	return lru
}

func (s *server) GetBlock(req *pb.GetBlockRequest, client pb.S3Proxy_GetBlockServer) error {
	prefix := utils.CommonPrefix(req.Info.ChainId, req.Info.Env, req.Info.BlockType == pb.BlockInfo_HEADER)
	lru := s.getOrInsertLru(prefix)
	key := ""
	if req.Info.BlockType == pb.BlockInfo_DATA {
		key = utils.BlockPrefix(req.Info)
	} else {
		key = utils.HeaderPrefix(req.Info)
	}
	buf, err := lru.Get(key, req.Info.BlockNum, s.s3GetBlock)
	if err != nil {
		return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
	}
	chunk := make([]byte, ChunkSize)
	reader := bytes.NewReader(buf)
	for {
		n, err := reader.Read(chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Errorf(utils.AwsS3ErrorCode, "GetBlock failed, err : %v", err)
		}
		if err := client.Send(&pb.BlockChunk{Chunk: chunk[:n]}); err != nil {
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
	prefix := utils.CommonPrefix(info.ChainId, info.Env, info.BlockType == pb.BlockInfo_HEADER)
	key := ""
	if info.BlockType == pb.BlockInfo_DATA {
		key = utils.BlockPrefix(info)
	} else {
		key = utils.HeaderPrefix(info)
	}
	rsp, err := s.s3.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buffer.Bytes()),
	})
	if err != nil {
		return status.Errorf(utils.AwsS3ErrorCode, "PutBlock failed, err : %v", err)
	}
	utils.Logger().Info("PutBlock", zap.String("key", key), zap.Any("rsp", rsp))
	lru := s.getOrInsertLru(prefix)
	lru.Insert(key, buffer.Bytes(), info.BlockNum)
	client.SendAndClose(&pb.PutBlockReply{})
	return nil
}

func (s *server) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileReply, error) {
	result, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(req.Path),
	})
	if result != nil {
		defer result.Body.Close()
	}
	if err != nil {
		return nil, status.Errorf(utils.AwsS3ErrorCode, "GetFile failed, err : %v", err)
	}
	buf, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, status.Errorf(utils.AwsS3ErrorCode, "GetFile failed, err : %v", err)
	}
	return &pb.GetFileReply{Data: buf}, nil
}

func (s *server) PutFile(ctx context.Context, req *pb.PutFileRequest) (*pb.PutFileReply, error) {
	rsp, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(req.Path),
		Body:   bytes.NewReader(req.Data),
	})
	if err != nil {
		return nil, status.Errorf(utils.AwsS3ErrorCode, "PutFile failed, err : %v", err)
	}
	utils.Logger().Info("PutFile", zap.String("key", req.Path), zap.Any("rsp", rsp))
	return &pb.PutFileReply{}, nil
}

func (s *server) ListHeaderStartAt(ctx context.Context, req *pb.ListHeaderStartAtRequest) (*pb.ListHeaderStartAtReply, error) {
	prefix := utils.CommonPrefix(req.ChainId, req.Env, true)
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
