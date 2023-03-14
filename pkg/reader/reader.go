package reader

import (
	"context"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/kafka"
	"github.com/DeBankDeFi/db-replicator/pkg/ndrc"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Reader struct {
	sync.Mutex

	config *utils.Config

	dbPool     *db.DBPool
	s3         *s3.Client
	kafka      *kafka.KafkaClient
	ndrcReader *ndrc.ReaderClient
	broker     *broker
	srv        *grpc.Server
	pb.UnimplementedRemoteServer

	lastBlockHeader *pb.BlockInfo
	resetC          <-chan string

	rootCtx   context.Context
	cancelFn  context.CancelFunc
	stopdoneC chan struct{} // Reader shutdown complete
}

func NewReader(config *utils.Config, dbPool *db.DBPool, resetChan <-chan string) (reader *Reader, err error) {
	s3, err := s3.NewClient(config.S3ProxyAddr)
	if err != nil {
		return nil, err
	}
	dbInfos, err := utils.OpenAndReadDbInfo(config.DBInfoPath)
	if err != nil {
		utils.Logger().Info("OpenAndReadDbInfo", zap.Any("err", err))
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

	env := lastBlockHeader.Env
	if env == "" {
		env = config.Env
	}

	chainId := lastBlockHeader.ChainId
	if chainId == "" {
		chainId = config.ChainId
	}

	role := lastBlockHeader.Role
	if role == "" {
		role = config.Role
	}

	if lastBlockHeader.BlockNum != -1 && lastBlockHeader.MsgOffset == -1 {
		infos, err := s3.ListHeaderStartAt(context.Background(), chainId, env, role,
			lastBlockHeader.BlockNum-1, 5, -1)
		if err != nil {
			utils.Logger().Error("ListHeaderStartAt error", zap.Error(err))
			return nil, err
		}
		for _, info := range infos {
			if info.BlockHash == lastBlockHeader.BlockHash || info.BlockNum == lastBlockHeader.BlockNum {
				lastBlockHeader = info
				break
			}
			if info.BlockNum == lastBlockHeader.BlockNum+1 {
				lastBlockHeader.MsgOffset = info.MsgOffset - 1
				break
			}
		}
	}

	topic := utils.Topic(env, chainId, role)

	kafka, err := kafka.NewKafkaClient(topic, lastBlockHeader.MsgOffset, config.KafkaAddr)
	if err != nil {
		return nil, err
	}
	ndrcReader, err := ndrc.NewReaderClient(config.NdrcAddr)
	if err != nil {
		return nil, err
	}

	rootCtx, cancelFn := context.WithCancel(context.Background())

	resetC := ndrcReader.WatchRole(rootCtx)

	reader = &Reader{
		config:          config,
		dbPool:          dbPool,
		s3:              s3,
		kafka:           kafka,
		ndrcReader:      ndrcReader,
		broker:          newBroker(),
		lastBlockHeader: lastBlockHeader,
		resetC:          resetC,
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
