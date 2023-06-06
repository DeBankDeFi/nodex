package s3_test

import (
	"context"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestS3(t *testing.T) {
	go s3.ListenAndServe("0.0.0.0:8765", 32)
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
	header0.BatchItems = append(header0.BatchItems, &pb.Data{
		Id: 0, Data: b})
	start := time.Now()
	err = client.PutBlock(context.Background(), header0)
	t.Logf("cost time: %v", time.Since(start))
	require.NoErrorf(t, err, "PutBlock error")

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			header, err := client.GetBlock(context.Background(), header0.Info, false)
			require.NoErrorf(t, err, "GetFile error")
			require.True(t, proto.Equal(header, header0))
			wg.Done()
		}()
	}
	wg.Wait()

	infos, err := client.ListHeaderStartAt(context.Background(), "256", "test", "master", 1, 1, -1)
	require.NoErrorf(t, err, "NewClient error")
	t.Logf("infos: %v", infos)
}
