package kafka

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/DeBankDeFi/nodex/pkg/metrics"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/utils"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	KafkaMaxBytes = 1 << 21
	KafkaMaxWait  = 500 * time.Millisecond
)

type KafkaClient struct {
	client           *kafka.Client
	writerLastOffset int64
	readerLastOffset int64
	topic            string
	kafkaMetrics     *metrics.KafkaMetrics
	sync.RWMutex
}

func NewKafkaClient(topic string, readerLastOffset int64, addrs ...string) (*KafkaClient, error) {
	kafka := &kafka.Client{
		Addr:    kafka.TCP(addrs...),
		Timeout: time.Second * 10,
	}
	client := &KafkaClient{
		client:           kafka,
		readerLastOffset: readerLastOffset,
		topic:            topic,
		kafkaMetrics:     metrics.NewKafkaMetrics(),
	}

	client.kafkaMetrics.SwitchTopic("init", topic)

	writeFirstOffset, writeLastOffset, err := client.RemoteOffset()
	if err != nil {
		return nil, err
	}
	if readerLastOffset > 0 {
		client.kafkaMetrics.IncreaseReaderOffset(topic, readerLastOffset)
	}
	if writeLastOffset > 0 {
		client.kafkaMetrics.IncreaseWriterOffset(topic, writeLastOffset)
	}
	utils.Logger().Info("remote offset", zap.Any("readerLastOffset", readerLastOffset), zap.Any("writeFirstOffset", writeFirstOffset), zap.Any("writeLastOffset ", writeLastOffset))

	if readerLastOffset < writeFirstOffset-1 {
		utils.Logger().Error("remote first offset less than reader last offset", zap.Any("readerLastOffset", readerLastOffset), zap.Any("writeFirstOffset", writeFirstOffset))
		return nil, errors.New("remote first offset less than reader last offset")
	}
	client.writerLastOffset = writeLastOffset
	return client, nil
}

func (k *KafkaClient) ResetTopic(topic string) {
	k.kafkaMetrics.SwitchTopic(k.topic, topic)
	k.topic = topic
}

func (k *KafkaClient) LastWriterOffset() int64 {
	return k.writerLastOffset
}

func (k *KafkaClient) RemoteOffset() (firstOffset int64, lastOffset int64, err error) {
	rsp, err := k.client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
		Addr: k.client.Addr,
		Topics: map[string][]kafka.OffsetRequest{
			k.topic: {
				{
					Partition: 0,
					Timestamp: kafka.FirstOffset,
				},
				{
					Partition: 0,
					Timestamp: kafka.LastOffset,
				},
			},
		},
	},
	)
	if err != nil {
		return -1, -1, err
	}
	return rsp.Topics[k.topic][0].FirstOffset, rsp.Topics[k.topic][0].LastOffset - 1, nil
}

func (k *KafkaClient) IncrementLastReaderOffset() {
	k.readerLastOffset++
	k.kafkaMetrics.IncreaseReaderOffset(k.topic, 1)
}

func (k *KafkaClient) LastReaderOffset() int64 {
	return k.readerLastOffset
}

func (k *KafkaClient) ResetLastReaderOffset(offset int64) {
	k.readerLastOffset = offset
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
	if k.writerLastOffset > 0 {
		k.kafkaMetrics.IncreaseWriterOffset(k.topic, 1)
	}
	utils.Logger().Info("broadcast", zap.Any("BaseOffset", rsp.BaseOffset), zap.Any("writerLastOffset", k.writerLastOffset))
	return nil
}

func (k *KafkaClient) Fetch(ctx context.Context) (infos []*pb.BlockInfo, err error) {
	return k.FetchStart(ctx, k.readerLastOffset+1)
}

func (k *KafkaClient) FetchStart(ctx context.Context, start int64) (infos []*pb.BlockInfo, err error) {
	records, err := k.fetch(ctx, start)
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

func (k *KafkaClient) fetch(ctx context.Context, start int64) (records []*kafka.Record, err error) {
	if start <= 0 {
		start = kafka.FirstOffset
	}
	startTime := time.Now()
	rsp, err := k.client.Fetch(ctx, &kafka.FetchRequest{
		Topic:     k.topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  KafkaMaxBytes,
		MaxWait:   KafkaMaxWait,
		Offset:    start,
	})

	if err != nil {
		utils.Logger().Error("fetch", zap.Any("err", err))
		return nil, err
	}
	if rsp.Error != nil {
		utils.Logger().Error("fetch", zap.Any("kafka err", rsp.Error))
		return nil, rsp.Error
	}
	k.kafkaMetrics.ObserveLatency(k.topic, float64(time.Since(startTime).Milliseconds()))
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
