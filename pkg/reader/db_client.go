package reader

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/pb"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ db.DB = &Remote{}

const ConnNum = 32

type Remote struct {
	clients [ConnNum]pb.RemoteClient
	id      int32
	count   int64
}

func NewClient(addr string) (pb.RemoteClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}
	return pb.NewRemoteClient(conn), nil
}

func OpenRemoteDB(addr string, dbType, path string, IsMetaDB bool) (db *Remote, err error) {
	db = &Remote{}
	for i := 0; i < ConnNum; i++ {
		client, err := NewClient(addr)
		if err != nil {
			return nil, err
		}
		db.clients[i] = client
	}
	rsp, err := db.getClient().Open(context.Background(), &pb.OpenRequest{
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

func (r *Remote) getClient() pb.RemoteClient {
	count := atomic.AddInt64(&r.count, 1)
	return r.clients[count%ConnNum]
}

func (r *Remote) Get(key []byte) (val []byte, err error) {
	rsp, err := r.getClient().Get(context.Background(), &pb.GetRequest{
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

func (r *Remote) Has(key []byte) (bool, error) {
	rsp, err := r.getClient().Get(context.Background(), &pb.GetRequest{
		Id:  r.id,
		Key: key,
	})
	if err != nil {
		return false, err
	}
	return rsp.Exist, nil
}

func (r *Remote) Put(key []byte, value []byte) (err error) {
	_, err = r.getClient().Put(context.Background(), &pb.PutRequest{
		Id:    r.id,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *Remote) Delete(key []byte) (err error) {
	_, err = r.getClient().Del(context.Background(), &pb.DelRequest{
		Id:  r.id,
		Key: key,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *Remote) Stat(property string) (stat string, err error) {
	rsp, err := r.getClient().Stat(context.Background(), &pb.StatRequest{
		Id:       r.id,
		Property: property,
	})
	if err != nil {
		return "", err
	}
	return rsp.Stat, nil
}

func (r *Remote) Stats() (stats map[string]string, err error) {
	rsp, err := r.getClient().Stats(context.Background(), &pb.StatsRequest{
		Id: r.id,
	})
	if err != nil {
		return nil, err
	}
	return rsp.Data, nil
}

func (r *Remote) Compact(start, limit []byte) (err error) {
	_, err = r.getClient().Compact(context.Background(), &pb.CompactRequest{
		Id:    r.id,
		Start: start,
		Limit: limit,
	})
	if err != nil {
		return err
	}
	return nil
}

func (r *Remote) NewIteratorWithRange(start, limit []byte) (iter db.Iterator, err error) {
	rsp, err := r.getClient().Iter(context.Background(), &pb.IterRequest{
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

func (r *Remote) NewIterator(prefix []byte, start []byte) (iter db.Iterator) {
	ran := db.BytesPrefixRange(prefix, start)
	rsp, err := r.getClient().Iter(context.Background(), &pb.IterRequest{
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
	rsp, err := r.getClient().Snapshot(context.Background())
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
	return &RemoteBatch{
		batch:  leveldb.MakeBatch(0),
		client: r.getClient(),
		id:     r.id,
	}
}

func (r *Remote) NewBatchWithSize(size int) db.Batch {
	return &RemoteBatch{
		batch:  leveldb.MakeBatch(size),
		client: r.getClient(),
		id:     r.id,
	}
}

func (r *Remote) Close() (err error) {
	_, err = r.getClient().Close(context.Background(), &pb.CloseRequest{
		Id: r.id,
	})
	if err != nil {
		return err
	}
	return nil
}
