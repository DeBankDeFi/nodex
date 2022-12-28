package kafka

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type KafkaClient struct {
	client           *kafka.Client
	writerLastOffset int64
	readerLastOffset int64
	topic            string
}

func NewKafkaClient(topic string, readerLastOffset int64, addrs ...string) (*KafkaClient, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(addrs...),
		Timeout: time.Second * 10,
	}
	rsp, err := client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
		Addr: client.Addr,
		Topics: map[string][]kafka.OffsetRequest{
			topic: {
				{
					Partition: 0,
					Timestamp:    kafka.LastOffset,
				},
			},
		},
	},
	)
	utils.Logger().Info("NewKafkaClient", zap.Any("rsp", rsp), zap.Any("err", err))
	if err != nil {
		return nil, err
	}
	return &KafkaClient{
		client:           client,
		writerLastOffset: rsp.Topics[topic][0].LastOffset-1,
		readerLastOffset: readerLastOffset,
		topic:            topic,
	}, nil
}

func (k *KafkaClient) ResetTopic(topic string) {
	k.topic = topic
}

func (k *KafkaClient) LastWriterOffset() int64 {
	return k.writerLastOffset
}

func (k *KafkaClient) IncrementLastReaderOffset() {
	k.readerLastOffset++
}

func (k *KafkaClient) LastReaderOffset() int64 {
	return k.readerLastOffset
}

func (k *KafkaClient) Broadcast(ctx context.Context, info *pb.BlockInfo) error {
	value, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	record := kafka.Record{
		Value: kafka.NewBytes(value),
	}
	return k.broadcast(ctx, record)
}

func (k *KafkaClient) broadcast(ctx context.Context, record kafka.Record) error {
	rsp, err := k.client.Produce(ctx, &kafka.ProduceRequest{
		Topic:        k.topic,
		Partition:    0,
		RequiredAcks: kafka.RequireAll,
		Records:      kafka.NewRecordReader(record),
	})
	if err != nil {
		return err
	}
	k.writerLastOffset++
	utils.Logger().Info("broadcast", zap.Any("BaseOffset", rsp.BaseOffset), zap.Any("writerLastOffset", k.writerLastOffset))
	return nil
}

func (k *KafkaClient) Fetch(ctx context.Context) (infos []*pb.BlockInfo, err error) {
	records, err := k.fetch(ctx)
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		info := &pb.BlockInfo{}
		buf, err := kafka.ReadAll(record.Value)
		if err != nil {
			return nil, err
		}
		if err := proto.Unmarshal(buf, info); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (k *KafkaClient) fetch(ctx context.Context) (records []*kafka.Record, err error) {
	rsp, err := k.client.Fetch(ctx, &kafka.FetchRequest{
		Topic:     k.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  1 << 24,
		MaxWait:   500 * time.Millisecond,
		Offset:    k.readerLastOffset + 1,
	})
	if err != nil {
		return nil, err
	}
	for {
		record, err := rsp.Records.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				break
			}
			return nil, err
		}
		utils.Logger().Info("fetch", zap.Any("offset", record.Offset), zap.Any("readerLastOffset", k.readerLastOffset))
		records = append(records, record)
	}
	return records, nil
}
