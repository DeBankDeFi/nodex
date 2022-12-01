package db

import (
	"encoding/json"
	"sync"

	"github.com/DeBankDeFi/db-replicator/pkg/utils"
)

// MemoryDB is a in-memory db
// only used in test
type MemoryDB struct {
	sync.Mutex
	db map[string][]byte
}

type MemoryBatch struct {
	db    *MemoryDB
	cache map[string][]byte
}

func (m *MemoryBatch) Put(key, value []byte) error {
	m.cache[string(key)] = value
	return nil
}

func (m *MemoryBatch) Delete(key []byte) error {
	m.cache[string(key)] = nil
	return nil
}

func (m *MemoryBatch) Load(data []byte) error {
	err := json.Unmarshal(data, &m.cache)
	if err != nil {
		return err
	}
	return nil
}

func (m *MemoryBatch) Write() error {
	m.db.Lock()
	defer m.db.Unlock()
	for k, v := range m.cache {
		m.db.db[k] = v
	}
	return nil
}

func (m *MemoryBatch) Dump() []byte {
	data, _ := json.Marshal(m.cache)
	return data
}

func NewMemoryDB() DB {
	return &MemoryDB{
		db: make(map[string][]byte),
	}
}

func (m *MemoryDB) Get(key []byte) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	if v, ok := m.db[string(key)]; !ok {
		return nil, utils.ErrNotFound
	} else {
		if v == nil {
			return nil, utils.ErrNotFound
		}
		return v, nil
	}
}

func (m *MemoryDB) NewBatch() Batch {
	return &MemoryBatch{
		db:    m,
		cache: make(map[string][]byte),
	}
}
