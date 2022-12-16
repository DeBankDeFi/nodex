package s3_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	ss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestS3(t *testing.T) {
	go s3.ListenAndServe("0.0.0.0:8765")
	client, err := s3.NewClient("0.0.0.0:8765")
	require.NoErrorf(t, err, "NewClient error")
	header0 := &pb.Block{
		Info: &pb.BlockInfo{
			ChainId:   "256",
			Env:       "test",
			BlockNum:  1,
			BlockHash: "1",
			MsgOffset: 0,
			BlockType: pb.BlockInfo_HEADER,
		}}
	b := make([]byte, 1<<22)
	rand.Read(b)
	header0.BatchItems = append(header0.BatchItems, &pb.BatchItem{
		Id: 0, Data: b})
	start := time.Now()
	err = client.PutFile(context.Background(), header0)
	t.Logf("cost time: %v", time.Since(start))
	require.NoErrorf(t, err, "PutFile error")

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			header, err := client.GetFile(context.Background(), header0.Info)
			require.NoErrorf(t, err, "GetFile error")
			require.True(t, proto.Equal(header, header0))
			wg.Done()
		}()
	}
	wg.Wait()

	infos, err := client.ListHeaderStartAt(context.Background(), "256", "test", 1, 1, -1)
	require.NoErrorf(t, err, "NewClient error")
	t.Logf("infos: %v", infos)
}

func TestS31(t *testing.T) {
	// go s3.ListenAndServe("0.0.0.0:8765")
	// client, err := s3.NewClient("0.0.0.0:8765")
	// require.NoErrorf(t, err, "NewClient error")
	// _, err = client.GetFile(context.Background(), &pb.BlockInfo{
	// 	ChainId:   "256",
	// 	Env:       "test",
	// 	BlockNum: 89,
	// 	BlockHash: "0x5851bc2d7f2a3d8032e1d4a17509a3c35a446857ab7893331929e51afec21bbc",
	// 	MsgOffset: 89,
	// 	BlockType: pb.BlockInfo_HEADER,
	// })
	// require.NoErrorf(t, err, "NewClient error")

	buf, err := os.ReadFile("/tmp/wal/89-2")
	require.NoErrorf(t, err, "NewClient error")
	header := &pb.Block{}
	err = proto.Unmarshal(buf, header)
	require.NoErrorf(t, err, "NewClient error")

	sdkConfig, err := config.LoadDefaultConfig(context.Background())
	require.NoErrorf(t, err, "NewClient error")
	s3 := ss3.NewFromConfig(sdkConfig)
	rsp, err := s3.GetObject(context.Background(), &ss3.GetObjectInput{
		Bucket: aws.String("blockchain-replicator"),
		Key:    aws.String("256/test/header/000000000089/000000000089/0x5851bc2d7f2a3d8032e1d4a17509a3c35a446857ab7893331929e51afec21bbc"),
	})
	require.NoErrorf(t, err, "NewClient error")
	b, err := io.ReadAll(rsp.Body)
	require.NoErrorf(t, err, "NewClient error")
	require.Equal(t, true, bytes.Equal(b, buf))
}
