package kafka_test

import (
	"context"
	"testing"

	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestKafka(t *testing.T) {
	client, err := kafka.NewKafkaClient("test", -1, "localhost:9092")
	require.NoError(t, err)

	info0 := &pb.BlockInfo{
		ChainId:  "256",
		BlockNum: 0,
	}
	info1 := &pb.BlockInfo{
		ChainId:  "256",
		BlockNum: 1,
	}
	info2 := &pb.BlockInfo{
		ChainId:  "256",
		BlockNum: 2,
	}
	err = client.Broadcast(context.Background(), info0)
	require.NoError(t, err)
	err = client.Broadcast(context.Background(), info1)
	require.NoError(t, err)
	err = client.Broadcast(context.Background(), info2)
	require.NoError(t, err)

	require.Equal(t, int64(2), client.LastWriterOffset())

	infos, err := client.Fetch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(infos))
	require.Equal(t, true, proto.Equal(info0, infos[0]))
	require.Equal(t, true, proto.Equal(info1, infos[1]))
	require.Equal(t, true, proto.Equal(info2, infos[2]))
	require.Equal(t, int64(2), client.LastReaderOffset())
}
