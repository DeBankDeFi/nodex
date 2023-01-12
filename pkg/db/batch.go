package db

type BatchWriter interface {
	// Write flushes any accumulated data to disk.
	Write() error
	// Load loads given slice into the batch.
	Load(data []byte) error
	// Dump dumps the batch into a byte slice.
	Dump() []byte
}

// Batch is a write-only put/del op set.
type Batch interface {
	KeyValueWriter
	BatchWriter

	// ValueSize retrieves the amount of data queued up for writing.
	ValueSize() int

	// Reset resets the batch for reuse.
	Reset()

	// Replay replays the batch contents.
	Replay(w KeyValueWriter) error
}

// Batcher wraps the NewBatch method of a backing data store.
type Batcher interface {
	// NewBatch creates a write-only database that buffers changes to its host db
	// until a final write is called.
	NewBatch() Batch

	// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
	NewBatchWithSize(size int) Batch
}

type BatchWithID struct {
	ID int32
	B  BatchWriter
}

type Replayer struct {
	Writer  KeyValueWriter
	Failure error
}

func (r *Replayer) Put(key, value []byte) {
	if r.Failure != nil {
		return
	}
	r.Failure = r.Writer.Put(key, value)
}

func (r *Replayer) Delete(key []byte) {
	if r.Failure != nil {
		return
	}
	r.Failure = r.Writer.Delete(key)
}
