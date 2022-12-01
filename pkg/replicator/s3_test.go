package replicator_test

import (
	"context"
	"testing"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/replicator"
	"github.com/stretchr/testify/require"
)

func TestS3(t *testing.T) {
	client, err := replicator.NewS3Client(context.Background(), "blockchain-heco", "test")
	require.NoError(t, err)
	start := time.Now()
	objects, err := client.ListHeaderStartAt(context.Background(), 0, 100, 0)
	require.NoError(t, err)
	t.Log(objects)
	t.Log(time.Now().UnixMilli() - start.UnixMilli())
}
