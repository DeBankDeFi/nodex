package utils

import (
	"sync"

	btree "github.com/google/btree"
)

const (
	UnInited     int = 0
	InProcess    int = 1
	Sucess       int = 2
	Failed       int = 3
	bucketsCount     = 512
)

type Cache struct {
	caches    map[string]*LRU
	cacheSize uint
	sync.RWMutex
}

func NewCache(cacheSize uint) *Cache {
	return &Cache{
		caches:    make(map[string]*LRU),
		cacheSize: cacheSize,
	}
}

func (c *Cache) GetOrCreatePrefixCache(prefix string) (lru *LRU) {
	c.RLock()
	if lru, ok := c.caches[prefix]; ok {
		c.RUnlock()
		return lru
	}
	c.RUnlock()
	lru = NewLru(c.cacheSize)
	c.Lock()
	if _, ok := c.caches[prefix]; !ok {
		c.caches[prefix] = lru
	} else {
		lru = c.caches[prefix]
	}
	c.Unlock()
	return lru
}

type FetchFn func() (value interface{}, err error)

// LRU implements a non-thread safe fixed size/weight LRU cache
type LRU struct {
	sync.RWMutex
	maxSize uint
	tree    *btree.BTreeG[*entry]
	items   map[string]*entry
}

// entry is used to hold a value in the evictList
type entry struct {
	key    string
	value  interface{}
	height int64
	cond   *sync.Cond
	state  int
}

func Less(item *entry, than *entry) bool {
	return item.height < than.height
}

func (item *entry) Value(fetchFn FetchFn) (value interface{}, err error) {
	if item.state == Sucess {
		return item.value, nil
	}
	item.cond.L.Lock()
	for item.state == InProcess {
		item.cond.Wait()
	}
	if item.state == Sucess {
		item.cond.L.Unlock()
		return item.value, nil
	} else {
		item.state = InProcess
		item.cond.L.Unlock()
		value, err = fetchFn()
		item.cond.L.Lock()
		if err != nil {
			item.state = Failed
			item.cond.L.Unlock()
			item.cond.Signal()
			return nil, err
		} else {
			item.value = value
			item.state = Sucess
			item.cond.L.Unlock()
			item.cond.Broadcast()
			return item.value, nil
		}
	}
}

// NewLru creates a weighted LRU of the given size.
func NewLru(maxSize uint) *LRU {
	c := &LRU{
		maxSize: maxSize,
		tree:    btree.NewG(32, Less),
		items:   make(map[string]*entry, maxSize),
	}
	return c
}

func (c *LRU) Insert(key string, value interface{}, height int64) *entry {
	c.Lock()
	defer c.Unlock()
	// Check for existing item
	if item, ok := c.items[key]; ok {
		if value != nil {
			item.value = value
			if item.height < height {
				c.tree.Delete(item)
				item.height = height
				c.tree.ReplaceOrInsert(item)
			}
			if item.cond != nil {
				item.cond.Broadcast()
			}
		}
		return item
	}

	// Add new item
	entry := &entry{key: key, height: height}
	if value == nil {
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
func (c *LRU) Get(key string, height int64, fetchFn FetchFn) (value interface{}, err error) {
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
