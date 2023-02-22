package etcd

import (
	"context"
	"fmt"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientV3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type EtcdClient struct {
	session   *concurrency.Session
	prefixKey string
}

func NewEtcdClient(endpoints []string, env, chainId string, ttl int) (*EtcdClient, error) {
	client, err := clientV3.New(clientV3.Config{
		Endpoints:            endpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    5 * time.Second,
		DialKeepAliveTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	key := fmt.Sprintf("%s/%s", env, chainId)
	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	session, err := concurrency.NewSession(client, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	return &EtcdClient{
		session:   session,
		prefixKey: key,
	}, nil
}

func (e *EtcdClient) Register(ctx context.Context, role string) (err error) {
	_, err = e.session.Client().Put(ctx, fmt.Sprintf("%s/%s", e.prefixKey, role), role, clientV3.WithLease(e.session.Lease()))
	if err != nil {
		return err
	}
	return err
}

func (e *EtcdClient) Unregister(ctx context.Context, role string) (err error) {
	_, err = e.session.Client().Delete(ctx, fmt.Sprintf("%s/%s", e.prefixKey, role))
	if err != nil {
		return err
	}
	return err
}

func (e *EtcdClient) SetLeader(ctx context.Context, role string) (err error) {
	_, err = e.session.Client().Put(ctx, fmt.Sprintf("%s/%s", e.prefixKey, role), role)
	if err != nil {
		return err
	}
	return err
}

func (e *EtcdClient) Leader(ctx context.Context) (string, error) {
	client := e.session.Client()
	resp, err := client.Get(ctx, e.prefixKey)
	if err != nil {
		return "", err
	} else if len(resp.Kvs) == 0 {
		return "", nil
	}
	for _, ev := range resp.Kvs {
		if string(ev.Key) == fmt.Sprintf("%s/%s", e.prefixKey, "master") {
			return string(ev.Value), nil
		}
	}
	return string(resp.Kvs[0].Value), nil
}

func (e *EtcdClient) WatchLeader(ctx context.Context) <-chan string {
	retc := make(chan string)
	go e.watch(ctx, retc)
	return retc
}

func (e *EtcdClient) watch(ctx context.Context, ch chan<- string) {
	client := e.session.Client()

	defer close(ch)
	for {
		resp, err := client.Get(ctx, e.prefixKey, clientV3.WithFirstCreate()...)
		if err != nil {
			return
		}

		var kv *mvccpb.KeyValue
		var hdr *pb.ResponseHeader

		if len(resp.Kvs) == 0 {
			cctx, cancel := context.WithCancel(ctx)
			opts := []clientV3.OpOption{clientV3.WithRev(resp.Header.Revision), clientV3.WithPrefix()}
			wch := client.Watch(cctx, e.prefixKey, opts...)
			for kv == nil {
				wr, ok := <-wch
				if !ok || wr.Err() != nil {
					cancel()
					return
				}
				for _, ev := range wr.Events {
					if ev.Type == mvccpb.PUT {
						hdr, kv = &wr.Header, ev.Kv
						hdr.Revision = kv.ModRevision
						break
					}
				}
			}
			cancel()
		} else {
			for _, ev := range resp.Kvs {
				if string(ev.Key) == fmt.Sprintf("%s/%s", e.prefixKey, "master") {
					kv = ev
					break
				}
			}
			hdr = resp.Header
			if kv == nil {
				kv = resp.Kvs[0]
			}
		}

		select {
		case ch <- string(kv.Value):
		case <-ctx.Done():
			return
		}

		cctx, cancel := context.WithCancel(ctx)
		wch := client.Watch(cctx, string(kv.Key), clientV3.WithRev(hdr.Revision+1))
		keyDeleted := false
		for !keyDeleted {
			wr, ok := <-wch
			if !ok {
				cancel()
				return
			}
			for _, ev := range wr.Events {
				if ev.Type == mvccpb.DELETE {
					keyDeleted = true
					break
				}
				select {
				case ch <- string(ev.Kv.Value):
				case <-cctx.Done():
					cancel()
					return
				}
			}
		}
		cancel()
	}
}

func (e *EtcdClient) Close() error {
	e.session.Close()
	e.session.Client().Close()
	return nil
}
