package reader

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/avast/retry-go/v4"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ db.DB = &Remote{}

func NewClient(addr string) (pb.RemoteClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	return pb.NewRemoteClient(conn), nil
}

type Remote struct {
	pool *utils.ClientPool
	id   int32
}

func OpenRemoteDB(addr string, dbType, path string, IsMetaDB bool) (db *Remote, err error) {
	db = &Remote{}
	pool, err := utils.NewClientPool(addr)
	if err != nil {
		return nil, err
	}
	db.pool = pool
	conn, _, err := db.pool.GetConn()
	if err != nil {
		return nil, err
	}
	dbClient := pb.NewRemoteClient(conn)
	rsp, err := dbClient.Open(context.Background(), &pb.OpenRequest{
		Type:     dbType,
		Path:     path,
		IsMetaDB: IsMetaDB,
	})
	if err != nil {
		return nil, err
	}
	db.id = rsp.Id
	return db, nil
}

func (r *Remote) Get(key []byte) (val []byte, err error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return nil, err
	}
	var rsp *pb.GetReply
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Get(context.Background(), &pb.GetRequest{
				Id:  r.id,
				Key: key,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if !rsp.Exist {
		return nil, leveldb.ErrNotFound
	}
	if rsp.Value == nil {
		rsp.Value = []byte{}
	}
	return rsp.Value, nil
}

func (r *Remote) Has(key []byte) (bool, error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return false, err
	}
	var rsp *pb.HasReply
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Has(context.Background(), &pb.HasRequest{
				Id:  r.id,
				Key: key,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	return rsp.Exist, nil
}

func (r *Remote) Put(key []byte, value []byte) (err error) {
	return nil
}

func (r *Remote) Delete(key []byte) (err error) {
	return nil
}

func (r *Remote) Stat(property string) (stat string, err error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return "", err
	}
	var rsp *pb.StatReply
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Stat(context.Background(), &pb.StatRequest{
				Id:       r.id,
				Property: property,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return "", err
	}
	return rsp.Stat, nil
}

func (r *Remote) Stats() (stats map[string]string, err error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return nil, err
	}
	var rsp *pb.StatsReply
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Stats(context.Background(), &pb.StatsRequest{
				Id: r.id,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}

func (r *Remote) Compact(start, limit []byte) (err error) {
	return nil
}

func (r *Remote) NewIteratorWithRange(start, limit []byte) (iter db.Iterator, err error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return nil, err
	}
	var rsp pb.Remote_IterClient
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Iter(context.Background(), &pb.IterRequest{
				Id:    r.id,
				Start: start,
				Limit: limit,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	return &RemoteIterator{
		client: rsp,
	}, nil
}

func (r *Remote) NewIterator(prefix []byte, start []byte) (iter db.Iterator) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return &RemoteIterator{
			err: err,
			end: true,
		}
	}
	ran := db.BytesPrefixRange(prefix, start)
	var rsp pb.Remote_IterClient
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Iter(context.Background(), &pb.IterRequest{
				Id:    r.id,
				Start: ran.Start,
				Limit: ran.Limit,
			})
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return &RemoteIterator{
			err: err,
			end: true,
		}
	}
	return &RemoteIterator{
		client: rsp,
	}
}

type RemoteIterator struct {
	client pb.Remote_IterClient
	key    []byte
	value  []byte
	end    bool
	err    error
}

func (r *RemoteIterator) Next() bool {
	if r.end {
		return false
	}
	rsp, err := r.client.Recv()
	if err != nil {
		r.err = err
		return false
	}
	r.key = rsp.Key
	r.value = rsp.Value
	r.end = rsp.IsEnd
	if rsp.Error != "" {
		r.err = fmt.Errorf("%s", rsp.Error)
	}
	return !r.end
}

func (r *RemoteIterator) Error() error {
	return r.err
}

func (r *RemoteIterator) Key() []byte {
	return r.key
}

func (r *RemoteIterator) Value() []byte {
	return r.value
}

func (r *RemoteIterator) Release() {
	r.client.CloseSend()
}

type RemoteSnapshot struct {
	client pb.Remote_SnapshotClient
	sync.Mutex
}

func (r *RemoteSnapshot) Get(key []byte) (val []byte, err error) {
	r.Lock()
	defer r.Unlock()
	err = r.client.Send(&pb.SnapshotRequest{
		Req: &pb.SnapshotRequest_Get{
			Get: &pb.GetRequest{
				Key: key,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	rsp, err := r.client.Recv()
	if err != nil {
		return nil, err
	}
	if rsp.GetGet().Exist {
		return rsp.GetGet().Value, nil
	} else {
		return nil, leveldb.ErrNotFound
	}
}

func (r *RemoteSnapshot) Has(key []byte) (has bool, err error) {
	r.Lock()
	defer r.Unlock()
	err = r.client.Send(&pb.SnapshotRequest{
		Req: &pb.SnapshotRequest_Has{
			Has: &pb.HasRequest{
				Key: key,
			},
		},
	})
	if err != nil {
		return false, err
	}
	rsp, err := r.client.Recv()
	if err != nil {
		return false, err
	}
	return rsp.GetGet().Exist, nil
}

func (r *RemoteSnapshot) Release() {
	r.Lock()
	defer r.Unlock()
	_ = r.client.Send(&pb.SnapshotRequest{
		Req: &pb.SnapshotRequest_Close{
			Close: &pb.CloseRequest{},
		},
	})
	r.client.Recv()
	r.client.CloseSend()
}

func (r *Remote) NewSnapshot() (snapshot db.Snapshot, err error) {
	conn, idx, err := r.pool.GetConn()
	if err != nil {
		return nil, err
	}
	var rsp pb.Remote_SnapshotClient
	err = retry.Do(
		func() error {
			if conn == nil {
				conn, idx, err = r.pool.GetConn()
				if err != nil {
					return err
				}
			}
			dbClient := pb.NewRemoteClient(conn)
			rsp, err = dbClient.Snapshot(context.Background())
			if err != nil {
				if utils.CheckConnState(conn) != nil {
					conn, err = r.pool.ResetConn(idx)
					if err != nil {
						return err
					}
				}
			}
			return err
		},
		retry.Attempts(5),
		retry.Delay(1*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		return nil, err
	}
	err = rsp.Send(&pb.SnapshotRequest{
		Id: r.id,
		Req: &pb.SnapshotRequest_Open{
			Open: &pb.SnapshotOpenRequest{},
		},
	})
	if err != nil {
		return nil, err
	}
	return &RemoteSnapshot{
		client: rsp,
	}, nil
}

type RemoteBatch struct {
	client pb.RemoteClient
	batch  *leveldb.Batch
	id     int32
	size   int
}

func (r *RemoteBatch) Put(key []byte, value []byte) error {
	r.batch.Put(key, value)
	r.size += len(key) + len(value)
	return nil
}

func (r *RemoteBatch) Delete(key []byte) error {
	r.batch.Delete(key)
	r.size += len(key)
	return nil
}

func (r *RemoteBatch) ValueSize() int {
	return r.size
}

func (r *RemoteBatch) Reset() {
	r.batch.Reset()
	r.size = 0
}

func (r *RemoteBatch) Replay(w db.KeyValueWriter) error {
	return r.batch.Replay(&db.Replayer{Writer: w})
}

func (r *RemoteBatch) Load(data []byte) error {
	return r.batch.Load(data)
}

func (r *RemoteBatch) Dump() []byte {
	return r.batch.Dump()
}

func (r *RemoteBatch) Write() error {
	_, err := r.client.Batch(context.Background(), &pb.BatchRequest{
		Id:   r.id,
		Data: r.batch.Dump(),
	})
	return err
}

func (r *Remote) NewBatch() db.Batch {
	return nil
}

func (r *Remote) NewBatchWithSize(size int) db.Batch {
	return nil
}

func (r *Remote) Close() (err error) {
	return nil
}
