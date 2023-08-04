package reader

import (
	"context"
	"io"
	"math"
	"net"

	"github.com/DeBankDeFi/nodex/pkg/db"
	"github.com/DeBankDeFi/nodex/pkg/pb"
	"github.com/DeBankDeFi/nodex/pkg/utils"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func (r *Reader) grpcRun(addr string) (*grpc.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(math.MaxInt32),
		grpc.MaxSendMsgSize(math.MaxInt32),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor))
	grpc_prometheus.EnableHandlingTimeHistogram()
	pb.RegisterRemoteServer(srv, r)
	r.srv = srv
	go func() {
		err := srv.Serve(ln)
		if err != nil {
			utils.Logger().Error("grpc server error", zap.Any("err", err))
		}
	}()
	return srv, nil
}

func (r *Reader) Open(ctx context.Context,
	req *pb.OpenRequest) (reply *pb.OpenReply, err error) {
	switch req.Type {
	case "leveldb":
		id, _ := r.dbPool.GetDBID(req.Path)
		if id >= 0 {
			return &pb.OpenReply{
				Id: id,
			}, nil
		}
		return nil, status.Errorf(utils.RemoteErrorCode, "%v db not found", req.Path)
	default:
		return nil, status.Errorf(utils.RemoteErrorCode, "unknown db type")
	}
}

func (r *Reader) Get(ctx context.Context,
	req *pb.GetRequest) (reply *pb.GetReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	value, err := db.Get(req.Key)
	if err == leveldb.ErrNotFound {
		return &pb.GetReply{
			Exist: false,
		}, nil
	}
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.GetReply{
		Value: value,
		Exist: true,
	}, nil
}

func (r *Reader) Has(ctx context.Context,
	req *pb.HasRequest) (reply *pb.HasReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	has, err := db.Has(req.Key)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.HasReply{
		Exist: has,
	}, nil
}

func (r *Reader) Put(ctx context.Context,
	req *pb.PutRequest) (reply *pb.PutReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	err = db.Put(req.Key, req.Value)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.PutReply{}, nil
}

func (r *Reader) Del(ctx context.Context,
	req *pb.DelRequest) (reply *pb.DelReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	err = db.Delete(req.Key)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.DelReply{}, nil
}

func (r *Reader) Stat(ctx context.Context,
	req *pb.StatRequest) (reply *pb.StatReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	stat, err := db.Stat(req.Property)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.StatReply{
		Stat: stat,
	}, nil
}

func (r *Reader) Stats(ctx context.Context,
	req *pb.StatsRequest) (reply *pb.StatsReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	stats, err := db.Stats()
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.StatsReply{
		Data: stats,
	}, nil
}

func (r *Reader) Compact(ctx context.Context,
	req *pb.CompactRequest) (reply *pb.CompactReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	err = db.Compact(req.Start, req.Limit)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.CompactReply{}, nil
}

func (r *Reader) Close(ctx context.Context,
	req *pb.CloseRequest) (reply *pb.CloseReply, err error) {
	return &pb.CloseReply{}, nil
}

func (r *Reader) Batch(ctx context.Context,
	req *pb.BatchRequest) (reply *pb.BatchReply, err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	batch := db.NewBatch()
	err = batch.Load(req.Data)
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	err = batch.Write()
	if err != nil {
		return nil, status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return &pb.BatchReply{}, nil
}

func (r *Reader) Iter(req *pb.IterRequest, client pb.Remote_IterServer) (err error) {
	db, err := r.dbPool.GetDB(req.Id)
	if err != nil {
		return status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	iter, err := db.NewIteratorWithRange(req.Start, req.Limit)
	if err != nil {
		return status.Errorf(utils.RemoteErrorCode, err.Error())
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
			return status.Errorf(utils.RemoteErrorCode, err.Error())
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
		return status.Errorf(utils.RemoteErrorCode, err.Error())
	}
	return nil
}

func (r *Reader) Snapshot(client pb.Remote_SnapshotServer) error {
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
			return status.Errorf(utils.RemoteErrorCode, "cannot receive: %v", err)
		}
		switch req.Req.(type) {
		case *pb.SnapshotRequest_Open:
			db, err := r.dbPool.GetDB(req.Id)
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
			}
			snapshot, err = db.NewSnapshot()
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
			}
		case *pb.SnapshotRequest_Close:
			snapshot.Release()
			err = client.Send(&pb.SnapshotReply{
				Reply: &pb.SnapshotReply_Close{
					Close: &pb.CloseReply{},
				}})
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
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
					return status.Errorf(utils.RemoteErrorCode, err.Error())
				}
				continue
			}
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
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
				return status.Errorf(utils.RemoteErrorCode, err.Error())
			}
		case *pb.SnapshotRequest_Has:
			has, err := snapshot.Has(req.Req.(*pb.SnapshotRequest_Has).Has.Key)
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
			}
			err = client.Send(&pb.SnapshotReply{
				Reply: &pb.SnapshotReply_Has{
					Has: &pb.HasReply{
						Exist: has,
					},
				},
			})
			if err != nil {
				return status.Errorf(utils.RemoteErrorCode, err.Error())
			}
		default:
			return status.Errorf(utils.RemoteErrorCode, "unknown request type")
		}
	}
}
