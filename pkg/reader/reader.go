package reader

import (
	"context"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type Reader struct {
	sync.Mutex

	config *utils.Config

	dbPool *db.DBPool
	s3     *s3.Client
	kafka  *kafka.KafkaClient
	broker *broker
	pb.UnimplementedRemoteServer
	srv *grpc.Server

	lastBlockHeader *pb.BlockInfo
	resetC          <-chan *utils.Config

	rootCtx   context.Context
	cancelFn  context.CancelFunc
	stopdoneC chan struct{} // Reader shutdown complete
}

func NewReader(config *utils.Config, dbPool *db.DBPool, resetChan <-chan string) (reader *Reader, err error) {
	s3, err := s3.NewClient(config.S3ProxyAddr)
	if err != nil {
		return nil, err
	}
	buf, err := s3.GetFile(context.Background(), utils.DBInfoPrefix(config.ChainId, config.Env))
	if err != nil {
		return nil, err
	}

	dbInfos := &pb.DBInfoList{}
	if err := proto.Unmarshal(buf, dbInfos); err != nil {
		return nil, err
	}

	utils.Logger().Info("NewReader", zap.Any("dbInfos", dbInfos))

	for _, dbInfo := range dbInfos.DbInfos {
		err = dbPool.Open(dbInfo, config.DBCacheSize)
		if err != nil {
			return nil, err
		}
	}

	lastBlockHeader, err := dbPool.GetBlockInfo()
	if err != nil {
		return nil, err
	}
	utils.Logger().Info("NewReader", zap.Int64("block_num", lastBlockHeader.BlockNum), zap.Int64("msg_offset", lastBlockHeader.MsgOffset))

	chainId := lastBlockHeader.ChainId
	if chainId == "" {
		chainId = config.ChainId
	}
	env := lastBlockHeader.Env
	if env == "" {
		env = config.Env
	}

	topic := utils.Topic(chainId, env)

	kafka, err := kafka.NewKafkaClient(topic, lastBlockHeader.MsgOffset, config.KafkaAddr)
	if err != nil {
		return nil, err
	}

	rootCtx, cancelFn := context.WithCancel(context.Background())
	reader = &Reader{
		config:          config,
		dbPool:          dbPool,
		s3:              s3,
		kafka:           kafka,
		broker:          newBroker(lastBlockHeader.MsgOffset),
		lastBlockHeader: lastBlockHeader,
		rootCtx:         rootCtx,
		cancelFn:        cancelFn,
		stopdoneC:       make(chan struct{}),
	}
	return reader, nil
}

func (r *Reader) Start() (err error) {
	go r.fetchRun()
	go r.grpcRun(r.config.RemoteListenAddr)
	return
}

func (r *Reader) Stop() {
	r.Lock()
	defer r.Unlock()
	r.cancelFn()
	r.srv.GracefulStop()
	<-r.stopdoneC
	r.dbPool.Close()
}
