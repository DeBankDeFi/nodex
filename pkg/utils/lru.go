package utils

import (
	"sync"

	btree "github.com/google/btree"
)

const (
	UnInited  int = 0
	InProcess int = 1
	Sucess    int = 2
	Failed    int = 3
)

type FetchFn func(key string) (value []byte, err error)

// Cache implements a non-thread safe fixed size/weight LRU cache
type Cache struct {
	sync.RWMutex
	maxSize uint
	tree    *btree.BTreeG[*entry]
	items   map[string]*entry
}

// entry is used to hold a value in the evictList
type entry struct {
	key    string
	value  []byte
	height int64
	cond   *sync.Cond
	state  int
}

func Less(item *entry, than *entry) bool {
	return item.height < than.height
}

func (item *entry) Value(fetchFn FetchFn) (buf []byte, err error) {
	if len(item.value) > 0 {
		return item.value, nil
	}
	item.cond.L.Lock()
	for item.state == InProcess {
		item.cond.Wait()
	}
	if item.state == Sucess || len(item.value) > 0 {
		item.state = Sucess
		item.cond.L.Unlock()
		return item.value, nil
	} else {
		item.state = InProcess
		item.cond.L.Unlock()
		buf, err = fetchFn(item.key)
		item.cond.L.Lock()
		if err != nil {
			if len(item.value) == 0 {
				item.state = Failed
				item.cond.L.Unlock()
				item.cond.Signal()
			} else {
				item.state = Sucess
				item.cond.L.Unlock()
				item.cond.Broadcast()
			}
			return nil, err
		} else {
			item.value = buf
			item.state = Sucess
			item.cond.L.Unlock()
			item.cond.Broadcast()
			return item.value, nil
		}
	}
}

// NewLru creates a weighted LRU of the given size.
func NewLru(maxSize uint) *Cache {
	c := &Cache{
		maxSize: maxSize,
		tree:    btree.NewG(32, Less),
		items:   make(map[string]*entry, maxSize),
	}
	return c
}

func (c *Cache) Insert(key string, value []byte, height int64) *entry {
	c.Lock()
	defer c.Unlock()
	// Check for existing item
	if item, ok := c.items[key]; ok {
		if len(value) > 0 {
			item.value = value
			if item.cond != nil {
				item.cond.Broadcast()
			}
		}
		return item
	}

	// Add new item
	entry := &entry{key: key, height: height}
	if len(value) == 0 {
		entry.state = UnInited
		entry.cond = sync.NewCond(&sync.Mutex{})
	} else {
		entry.state = Sucess
		entry.value = value
	}
	c.items[key] = entry
	c.tree.ReplaceOrInsert(entry)
	if c.tree.Len() > int(c.maxSize) {
		item, ok := c.tree.DeleteMin()
		if ok {
			delete(c.items, item.key)
		}
	}
	return entry
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key string, height int64, fetchFn FetchFn) (value []byte, err error) {
	c.RLock()
	if item, ok := c.items[key]; ok {
		c.RUnlock()
		return item.Value(fetchFn)
	} else {
		c.RUnlock()
		item := c.Insert(key, nil, height)
		return item.Value(fetchFn)
	}
}
