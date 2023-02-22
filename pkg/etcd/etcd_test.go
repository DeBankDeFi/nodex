package etcd_test

import (
	"context"
	"testing"

	"github.com/DeBankDeFi/db-replicator/pkg/etcd"
)

func TestEtcd(t *testing.T) {
	client, err := etcd.NewEtcdClient([]string{"127.0.0.1:2379"}, "test", "256", 10)
	if err != nil {
		t.Fatal(err)
	}
	ch := client.WatchLeader(context.Background())
	client.SetLeader(context.Background(), "master")
	for a := range ch {
		t.Log(a)
		break
	}

}
