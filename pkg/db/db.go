package db

// DB is the interface that wraps the basic operations of a key-value data store.
// only list db-replicator related methods
type DB interface {
	// Get returns the value for the given key if it's present.
	Get(key []byte) ([]byte, error)

	// NewBatch creates a write-only database that buffers changes to its host db.
	NewBatch() Batch
}
