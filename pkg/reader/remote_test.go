package reader_test

// import (
// 	"testing"

// 	"github.com/DeBankDeFi/db-replicator/pkg/db"
// 	"github.com/DeBankDeFi/db-replicator/pkg/pb"
// 	"github.com/DeBankDeFi/db-replicator/pkg/reader"
// 	"github.com/stretchr/testify/require"
// 	"github.com/syndtr/goleveldb/leveldb"
// )

// func TestRemoteSimple(t *testing.T) {
// 	path := t.TempDir() + "/" + "test1"
// 	pool := db.NewDBPool()
// 	err := pool.Open(&pb.DBInfo{
// 		Id:     0,
// 		DbType: "leveldb",
// 		DbPath: path,
// 		IsMeta: true,
// 	})
// 	require.NoErrorf(t, err, "Open error")
// 	go reader.ListenAndServe("0.0.0.0:7651", pool)
// 	client, err := reader.NewClient("0.0.0.0:7651")
// 	require.NoErrorf(t, err, "NewClient error")
// 	db, err := reader.OpenRemoteDB(client, "leveldb", path, true)
// 	require.NoErrorf(t, err, "OpenRemoteDB error")
// 	_, err = db.Get([]byte("key"))
// 	require.ErrorIs(t, err, leveldb.ErrNotFound)
// 	err = db.Put([]byte("key"), []byte("value"))
// 	require.NoErrorf(t, err, "Put error")
// 	val, err := db.Get([]byte("key"))
// 	require.NoErrorf(t, err, "Get error")
// 	require.Equal(t, []byte("value"), val)
// 	has, err := db.Has([]byte("key"))
// 	require.NoErrorf(t, err, "Has error")
// 	require.True(t, has)
// }

// func TestRemoteSnapshot(t *testing.T) {
// 	path := t.TempDir() + "/" + "test2"
// 	pool := db.NewDBPool()
// 	err := pool.Open(&pb.DBInfo{
// 		Id:     0,
// 		DbType: "leveldb",
// 		DbPath: path,
// 		IsMeta: true,
// 	})
// 	t.Logf("path: %s", path)
// 	require.NoErrorf(t, err, "Open error")
// 	go reader.ListenAndServe("0.0.0.0:7652", pool)
// 	client, err := reader.NewClient("0.0.0.0:7652")
// 	require.NoErrorf(t, err, "NewClient error")
// 	db, err := reader.OpenRemoteDB(client, "leveldb", path, true)
// 	require.NoErrorf(t, err, "OpenRemoteDB error")
// 	_, err = db.Get([]byte("key"))
// 	require.ErrorIs(t, err, leveldb.ErrNotFound)
// 	err = db.Put([]byte("key"), []byte("1"))
// 	require.NoErrorf(t, err, "Put error")
// 	snapshot, err := db.NewSnapshot()
// 	require.NoErrorf(t, err, "NewSnapshot error")
// 	val, err := snapshot.Get([]byte("key"))
// 	require.NoErrorf(t, err, "Get error")
// 	require.Equal(t, []byte("1"), val)
// 	err = db.Put([]byte("key"), []byte("2"))
// 	require.NoErrorf(t, err, "Put error")
// 	val, err = snapshot.Get([]byte("key"))
// 	require.NoErrorf(t, err, "Get error")
// 	require.Equal(t, []byte("1"), val)
// 	val, err = db.Get([]byte("key"))
// 	require.NoErrorf(t, err, "Get error")
// 	require.Equal(t, []byte("2"), val)
// }

// func TestRemoteBatch(t *testing.T) {
// 	path := t.TempDir() + "/" + "test3"
// 	pool := db.NewDBPool()
// 	err := pool.Open(&pb.DBInfo{
// 		Id:     0,
// 		DbType: "leveldb",
// 		DbPath: path,
// 		IsMeta: true,
// 	})
// 	require.NoErrorf(t, err, "Open error")
// 	go reader.ListenAndServe("0.0.0.0:7653", pool)
// 	client, err := reader.NewClient("0.0.0.0:7653")
// 	require.NoErrorf(t, err, "NewClient error")
// 	db, err := reader.OpenRemoteDB(client, "leveldb", path, true)
// 	require.NoErrorf(t, err, "OpenRemoteDB error")
// 	batch := db.NewBatch()
// 	batch.Put([]byte("key1"), []byte("value1"))
// 	batch.Put([]byte("key2"), []byte("value2"))
// 	batch.Put([]byte("key3"), []byte("value3"))
// 	err = batch.Write()
// 	require.NoErrorf(t, err, "Write error")
// 	val, err := db.Get([]byte("key1"))
// 	require.NoErrorf(t, err, "Get error")
// 	require.Equal(t, []byte("value1"), val)
// }

// func TestRemoteIter(t *testing.T) {
// 	path := t.TempDir() + "/" + "test4"
// 	pool := db.NewDBPool()
// 	err := pool.Open(&pb.DBInfo{
// 		Id:     0,
// 		DbType: "leveldb",
// 		DbPath: path,
// 		IsMeta: true,
// 	})
// 	require.NoErrorf(t, err, "Open error")
// 	go reader.ListenAndServe("0.0.0.0:7654", pool)
// 	client, err := reader.NewClient("0.0.0.0:7654")
// 	require.NoErrorf(t, err, "NewClient error")
// 	db, err := reader.OpenRemoteDB(client, "leveldb", path, true)
// 	require.NoErrorf(t, err, "OpenRemoteDB error")
// 	batch := db.NewBatch()
// 	batch.Put([]byte("1"), []byte("1"))
// 	batch.Put([]byte("2"), []byte("2"))
// 	batch.Put([]byte("3"), []byte("3"))
// 	err = batch.Write()
// 	require.NoErrorf(t, err, "Write error")
// 	iter, err := db.NewIteratorWithRange([]byte("1"), []byte("4"))
// 	require.NoErrorf(t, err, "Iter error")
// 	for iter.Next() {
// 		t.Log(iter.Key(), iter.Value())
// 	}
// }
