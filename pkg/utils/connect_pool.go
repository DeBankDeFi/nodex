package utils

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
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
	pool.connectAll()
	return pool, nil
}

func CheckConnState(conn *grpc.ClientConn) error {
	state := conn.GetState()
	switch state {
	case connectivity.TransientFailure, connectivity.Shutdown:
		return fmt.Errorf("grpc connection state is %s", state.String())
	}

	return nil
}

func (cc *ClientPool) connectAll() error {
	for i := 0; i < ConnNum; i++ {
		conn, err := grpc.Dial(cc.target, grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32),
				grpc.MaxCallSendMsgSize(math.MaxInt32)))
		if err != nil {
			return err
		}
		cc.conns[i] = conn
	}
	return nil
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

	cc.connectAll()
	conn = cc.conns[idx]
	return conn, nil
}
