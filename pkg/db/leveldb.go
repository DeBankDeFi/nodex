package db

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LDB struct {
	Id int32
	DB *leveldb.DB
}

type LBatch struct {
	DB    *leveldb.DB
	Batch *leveldb.Batch
	size  int
}

func (l *LBatch) Put(key, value []byte) error {
	l.Batch.Put(key, value)
	l.size += len(key) + len(value)
	return nil
}

func (l *LBatch) Delete(key []byte) error {
	l.Batch.Delete(key)
	l.size += len(key)
	return nil
}

func (l *LBatch) ValueSize() int {
	return l.size
}

func (l *LBatch) Write() error {
	return l.DB.Write(l.Batch, nil)
}

func (l *LBatch) Load(data []byte) error {
	return l.Batch.Load(data)
}

func (l *LBatch) Dump() []byte {
	return l.Batch.Dump()
}

func (l *LBatch) Replay(w KeyValueWriter) error {
	return l.Batch.Replay(&Replayer{Writer: w})
}

func (l *LBatch) Reset() {
	l.Batch.Reset()
	l.size = 0
}

type LIterator struct {
	iter iterator.Iterator
}

func (iter *LIterator) Next() bool {
	return iter.iter.Next()
}

func (iter *LIterator) Key() []byte {
	return iter.iter.Key()
}

func (iter *LIterator) Error() error {
	return iter.iter.Error()
}

func (iter *LIterator) Value() []byte {
	return iter.iter.Value()
}

func (iter *LIterator) Release() {
	iter.iter.Release()
}

type LSnapshot struct {
	snap *leveldb.Snapshot
}

func (snap *LSnapshot) Get(key []byte) ([]byte, error) {
	return snap.snap.Get(key, nil)
}

func (snap *LSnapshot) Has(key []byte) (bool, error) {
	return snap.snap.Has(key, nil)
}

func (snap *LSnapshot) Release() {
	if snap.snap != nil {
		snap.snap.Release()
		snap.snap = nil
	}
}

func NewLDB(path string) (*LDB, error) {
	// Set default options
	options := &opt.Options{
		Filter:                 filter.NewBloomFilter(10),
		DisableSeeksCompaction: true,
	}
	// Open the db and recover any potential corruptions
	db, err := leveldb.OpenFile(path, options)
	if _, corrupted := err.(*errors.ErrCorrupted); corrupted {
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}
	return &LDB{DB: db}, nil
}

func (l *LDB) Get(key []byte) ([]byte, error) {
	return l.DB.Get(key, nil)
}

func (l *LDB) Has(key []byte) (bool, error) {
	return l.DB.Has(key, nil)
}

func (l *LDB) Put(key, value []byte) error {
	return l.DB.Put(key, value, nil)
}

func (l *LDB) Delete(key []byte) error {
	return l.DB.Delete(key, nil)
}

func (l *LDB) Stat(property string) (string, error) {
	return l.DB.GetProperty(property)
}

func (l *LDB) Stats() (map[string]string, error) {
	keys := []string{
		"leveldb.num-files-at-level{n}",
		"leveldb.stats",
		"leveldb.sstables",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.openedtables",
		"leveldb.alivesnaps",
		"leveldb.aliveiters",
	}

	stats := make(map[string]string)
	for _, key := range keys {
		str, err := l.DB.GetProperty(key)
		if err == nil {
			stats[key] = str
		} else {
			return nil, err
		}
	}
	return stats, nil
}

func (l *LDB) Compact(start, limit []byte) error {
	return l.DB.CompactRange(util.Range{Start: start, Limit: limit})
}

func (l *LDB) NewBatch() Batch {
	return &LBatch{
		DB:    l.DB,
		Batch: new(leveldb.Batch),
	}
}

func (l *LDB) NewBatchWithSize(size int) Batch {
	return &LBatch{
		DB:    l.DB,
		Batch: leveldb.MakeBatch(size),
	}
}

func (l *LDB) NewIteratorWithRange(start, limit []byte) (Iterator, error) {
	iter := l.DB.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
	return &LIterator{iter: iter}, nil
}

func (l *LDB) NewIterator(prefix []byte, start []byte) Iterator {
	iter := l.DB.NewIterator(BytesPrefixRange(prefix, start), nil)
	return &LIterator{iter: iter}
}

func (l *LDB) NewSnapshot() (Snapshot, error) {
	snap, err := l.DB.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &LSnapshot{snap: snap}, nil
}

func (l *LDB) Close() error {
	return l.DB.Close()
}
