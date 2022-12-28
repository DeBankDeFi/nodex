package remote

import (
	"context"
	"io"
	"math"
	"net"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedRemoteDBServer
	sync.Mutex
	pool *db.DBPool
}

func ListenAndServe(addr string, pool *db.DBPool) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv, err := NewServer(pool)
	if err != nil {
		return err
	}
	return srv.Serve(ln)
}

func NewServer(pool *db.DBPool) (*grpc.Server, error) {
	s := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32))
	pb.RegisterRemoteDBServer(s, &server{
		pool: pool,
	})
	return s, nil
}

func (s *server) Open(ctx context.Context,
	req *pb.OpenRequest) (reply *pb.OpenReply, err error) {
	switch req.Type {
	case "leveldb":
		id, _ := s.pool.GetDBID(req.Path)
		if id >= 0 {
			return &pb.OpenReply{
				Id: id,
			}, nil
		}
		return nil, status.Errorf(utils.RemoteDBErrorCode, "db not found")
	default:
		return nil, status.Errorf(utils.RemoteDBErrorCode, "unknown db type")
	}
}

func (s *server) Get(ctx context.Context,
	req *pb.GetRequest) (reply *pb.GetReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	value, err := db.Get(req.Key)
	if err == leveldb.ErrNotFound {
		return &pb.GetReply{
			Exist: false,
		}, nil
	}
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.GetReply{
		Value: value,
		Exist: true,
	}, nil
}

func (s *server) Has(ctx context.Context,
	req *pb.HasRequest) (reply *pb.HasReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	has, err := db.Has(req.Key)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.HasReply{
		Exist: has,
	}, nil
}

func (s *server) Put(ctx context.Context,
	req *pb.PutRequest) (reply *pb.PutReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	err = db.Put(req.Key, req.Value)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.PutReply{}, nil
}

func (s *server) Delete(ctx context.Context,
	req *pb.DelRequest) (reply *pb.DelReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	err = db.Delete(req.Key)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.DelReply{}, nil
}

func (s *server) Stat(ctx context.Context,
	req *pb.StatRequest) (reply *pb.StatReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	stat, err := db.Stat(req.Property)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.StatReply{
		Stat: stat,
	}, nil
}

func (s *server) Stats(ctx context.Context,
	req *pb.StatsRequest) (reply *pb.StatsReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	stats, err := db.Stats()
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.StatsReply{
		Data: stats,
	}, nil
}

func (s *server) Compact(ctx context.Context,
	req *pb.CompactRequest) (reply *pb.CompactReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	err = db.Compact(req.Start, req.Limit)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.CompactReply{}, nil
}

func (s *server) Close(ctx context.Context,
	req *pb.CloseRequest) (reply *pb.CloseReply, err error) {
	return &pb.CloseReply{}, nil
}

func (s *server) Batch(ctx context.Context,
	req *pb.BatchRequest) (reply *pb.BatchReply, err error) {
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	batch := db.NewBatch()
	err = batch.Load(req.Data)
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	err = batch.Write()
	if err != nil {
		return nil, status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return &pb.BatchReply{}, nil
}

func (s *server) Iter(req *pb.IterRequest, client pb.RemoteDB_IterServer) (err error) {
	utils.Logger().Info("Iter", zap.Any("req", req))
	db, err := s.pool.GetDB(req.Id)
	if err != nil {
		return status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	iter, err := db.NewIteratorWithRange(req.Start, req.Limit)
	if err != nil {
		return status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	defer iter.Release()

	for iter.Next() {
		reply := &pb.IterReply{
			Key:   iter.Key(),
			Value: iter.Value(),
			IsEnd: false,
		}
		if iter.Error() != nil {
			reply.Error = iter.Error().Error()
		}
		err = client.Send(reply)
		if err != nil {
			return status.Errorf(utils.RemoteDBErrorCode, err.Error())
		}
	}
	reply := &pb.IterReply{
		IsEnd: true,
	}
	if iter.Error() != nil {
		reply.Error = iter.Error().Error()
	}
	err = client.Send(reply)
	if err != nil {
		return status.Errorf(utils.RemoteDBErrorCode, err.Error())
	}
	return nil
}

func (s *server) Snapshot(client pb.RemoteDB_SnapshotServer) error {
	var snapshot db.Snapshot
	defer func() {
		if snapshot != nil {
			snapshot.Release()
		}
	}()
	for {
		req, err := client.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(utils.RemoteDBErrorCode, "cannot receive: %v", err)
		}
		switch req.Req.(type) {
		case *pb.SnapshotRequest_Open:
			db, err := s.pool.GetDB(req.Id)
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
			snapshot, err = db.NewSnapshot()
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
		case *pb.SnapshotRequest_Close:
			snapshot.Release()
			err = client.Send(&pb.SnapshotReply{
				Reply: &pb.SnapshotReply_Close{
					Close: &pb.CloseReply{},
				}})
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}

		case *pb.SnapshotRequest_Get:
			val, err := snapshot.Get(req.Req.(*pb.SnapshotRequest_Get).Get.Key)
			if err == leveldb.ErrNotFound {
				err = client.Send(&pb.SnapshotReply{
					Reply: &pb.SnapshotReply_Get{
						Get: &pb.GetReply{
							Value: val,
							Exist: false,
						},
					},
				})
				if err != nil {
					return status.Errorf(utils.RemoteDBErrorCode, err.Error())
				}
				continue
			}
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
			err = client.Send(&pb.SnapshotReply{
				Reply: &pb.SnapshotReply_Get{
					Get: &pb.GetReply{
						Value: val,
						Exist: true,
					},
				},
			})
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
		case *pb.SnapshotRequest_Has:
			has, err := snapshot.Has(req.Req.(*pb.SnapshotRequest_Has).Has.Key)
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
			err = client.Send(&pb.SnapshotReply{
				Reply: &pb.SnapshotReply_Has{
					Has: &pb.HasReply{
						Exist: has,
					},
				},
			})
			if err != nil {
				return status.Errorf(utils.RemoteDBErrorCode, err.Error())
			}
		default:
			return status.Errorf(utils.RemoteDBErrorCode, "unknown request type")
		}
	}
}
