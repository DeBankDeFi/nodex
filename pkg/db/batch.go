package db

// Batch is a write-only put/del op set.
type Batch interface {
	// Put inserts the given value into the key-value data store.
	Put(key []byte, value []byte) error

	// Delete removes the key from the key-value data store.
	Delete(key []byte) error

	// Write flushes any accumulated data to disk.
	Write() error

	// Load loads given slice into the batch.
	Load(data []byte) error

	// Dump dumps the batch into a byte slice.
	Dump() []byte
}

type BatchWithID struct {
	ID int32
	B  Batch
}
