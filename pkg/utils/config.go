package utils

import (
	"go.uber.org/zap"
)

// Config represents the configuration of the replicator.
type Config struct {
	Logger    *zap.Logger
	Bucket    string
	EnvPrex   string
	ReorgDeep int32
}

// NewDevelopmentConfig returns a Dev env Config with default values.
func NewDevelopmentConfig() *Config {
	return &Config{
		Bucket:    "blockchain-heco",
		EnvPrex:   "test",
		ReorgDeep: 128,
		Logger:    Logger(),
	}
}
