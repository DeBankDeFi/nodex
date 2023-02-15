package utils

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrNotFoundClient = errors.New("not found grpc conn")
	ErrConnShutdown   = errors.New("grpc conn shutdown")
)

const ConnNum = 32

type ClientPool struct {
	sync.Mutex
	next   int64
	target string
	conns  [ConnNum]*grpc.ClientConn
}

func NewClientPool(target string) (*ClientPool, error) {
	pool := &ClientPool{
		next:   0,
		target: target,
	}
	for i := 0; i < ConnNum; i++ {
		client, err := pool.connect()
		if err != nil {
			return nil, err
		}
		pool.conns[i] = client
	}
	return pool, nil
}

func CheckConnState(conn *grpc.ClientConn) error {
	state := conn.GetState()
	switch state {
	case connectivity.TransientFailure, connectivity.Shutdown:
		return ErrConnShutdown
	}

	return nil
}

func (cc *ClientPool) connect() (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(cc.target, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
			grpc.MaxCallSendMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (cc *ClientPool) GetConn() (*grpc.ClientConn, int64, error) {
	var (
		idx  int64
		next int64
	)

	next = atomic.AddInt64(&cc.next, 1)
	idx = next % ConnNum
	conn := cc.conns[idx]
	if conn != nil {
		return conn, idx, nil
	}

	conn, err := cc.ResetConn(idx)
	return conn, idx, err
}

func (cc *ClientPool) ResetConn(idx int64) (*grpc.ClientConn, error) {
	cc.Lock()
	defer cc.Unlock()

	conn := cc.conns[idx]
	if conn != nil && CheckConnState(conn) == nil {
		return conn, nil
	}

	conn, err := cc.connect()
	if err != nil {
		return nil, err
	}

	cc.conns[idx] = conn
	return conn, nil
}
