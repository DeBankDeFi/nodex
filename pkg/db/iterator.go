package db

import "github.com/syndtr/goleveldb/leveldb/util"

type Iterator interface {
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done. The caller
	// should not modify the contents of the returned slice, and its contents may
	// change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done. The
	// caller should not modify the contents of the returned slice, and its contents
	// may change on the next call to Next.
	Value() []byte

	// Release releases associated resources. Release should always succeed and can
	// be called multiple times without causing error.
	Release()
}

// BytesPrefixRange returns key range that satisfy
// - the given prefix, and
// - the given seek position
func BytesPrefixRange(prefix, start []byte) *util.Range {
	r := util.BytesPrefix(prefix)
	r.Start = append(r.Start, start...)
	return r
}
