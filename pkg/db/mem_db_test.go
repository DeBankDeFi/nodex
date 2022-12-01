package db_test

import (
	"testing"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestMemDB(t *testing.T) {
	// create a new memory db
	db := db.NewMemoryDB()

	// get noexist key
	val, err := db.Get([]byte("key"))
	require.ErrorAs(t, err, &utils.ErrNotFound)
	require.Nil(t, val)

	// put
	batch := db.NewBatch()
	err = batch.Put([]byte("key"), []byte("value"))
	require.NoError(t, err)
	err = batch.Write()
	require.NoError(t, err)

	val, err = db.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, val, []byte("value"))

	// delete
	batch = db.NewBatch()
	err = batch.Delete([]byte("key"))
	require.NoError(t, err)
	err = batch.Write()
	require.NoError(t, err)

	val, err = db.Get([]byte("key"))
	require.ErrorAs(t, err, &utils.ErrNotFound)
	require.Nil(t, val)
}
