package utils

import "errors"

var (
	ErrNotFound = errors.New("db: not found")

	ErrNoMetaDBRegistered = errors.New("db: no meta db registered")

	ErrWriterStopped = errors.New("writer: writer stopped")
)
