package remote

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/utils/pb"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ db.DB = &RemoteDB{}

type RemoteDB struct {
	client pb.RemoteDBClient
	id     int32
}

func NewClient(addr string) (pb.RemoteDBClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	return pb.NewRemoteDBClient(conn), nil
}

func OpenRemoteDB(client pb.RemoteDBClient, dbType, path string, IsMetaDB bool) (db *RemoteDB, err error) {
	rsp, err := client.Open(context.Background(), &pb.OpenRequest{
		Type:     dbType,
		Path:     path,
		IsMetaDB: IsMetaDB,
	})
	if err != nil {
		return nil, err
	}
	return &RemoteDB{
		client: client,
		id:     rsp.Id,
	}, nil
}

func (r *RemoteDB) Get(key []byte) (val []byte, err error) {
	rsp, err := r.client.Get(context.Background(), &pb.GetRequest{
		Id:  r.id,
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	if !rsp.Exist {
		return nil, leveldb.ErrNotFound
	}
	if rsp.Value == nil {
		rsp.Value = []byte{}
	}
	return rsp.Value, nil
}

func (r *RemoteDB) Has(key []byte) (bool, error) {
	rsp, err := r.client.Get(context.Background(), &pb.GetRequest{
		Id:  r.id,
		Key: key,
	})
	if err != nil {
		return false, err
	}
	return rsp.Exist, nil
}

func (r *RemoteDB) Put(key []byte, value []byte) (err error) {
	_, err = r.client.Put(context.Background(), &pb.PutRequest{
		Id:    r.id,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *RemoteDB) Delete(key []byte) (err error) {
	_, err = r.client.Del(context.Background(), &pb.DelRequest{
		Id:  r.id,
		Key: key,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *RemoteDB) Stat(property string) (stat string, err error) {
	rsp, err := r.client.Stat(context.Background(), &pb.StatRequest{
		Id:       r.id,
		Property: property,
	})
	if err != nil {
		return "", err
	}
	return rsp.Stat, nil
}

func (r *RemoteDB) Stats() (stats map[string]string, err error) {
	rsp, err := r.client.Stats(context.Background(), &pb.StatsRequest{
		Id: r.id,
	})
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}

func (r *RemoteDB) Compact(start, limit []byte) (err error) {
	_, err = r.client.Compact(context.Background(), &pb.CompactRequest{
		Id:    r.id,
		Start: start,
		Limit: limit,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *RemoteDB) NewIteratorWithRange(start, limit []byte) (iter db.Iterator, err error) {
	rsp, err := r.client.Iter(context.Background(), &pb.IterRequest{
		Id:    r.id,
		Start: start,
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}
	return &RemoteIterator{
		client: rsp,
	}, nil
}

func (r *RemoteDB) NewIterator(prefix []byte, start []byte) (iter db.Iterator) {
	ran := db.BytesPrefixRange(prefix, start)
	rsp, err := r.client.Iter(context.Background(), &pb.IterRequest{
		Id:    r.id,
		Start: ran.Start,
		Limit: ran.Limit,
	})
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
	client pb.RemoteDB_IterClient
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
	client pb.RemoteDB_SnapshotClient
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

func (r *RemoteDB) NewSnapshot() (snapshot db.Snapshot, err error) {
	rsp, err := r.client.Snapshot(context.Background())
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
	client pb.RemoteDBClient
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

func (r *RemoteDB) NewBatch() db.Batch {
	return &RemoteBatch{
		batch:  leveldb.MakeBatch(0),
		client: r.client,
		id:     r.id,
	}
}

func (r *RemoteDB) NewBatchWithSize(size int) db.Batch {
	return &RemoteBatch{
		batch:  leveldb.MakeBatch(size),
		client: r.client,
		id:     r.id,
	}
}

func (r *RemoteDB) Close() (err error) {
	_, err = r.client.Close(context.Background(), &pb.CloseRequest{
		Id: r.id,
	})
	if err != nil {
		return err
	}
	return nil
}
