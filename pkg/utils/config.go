package utils

// Config represents the configuration of the replicator.
type Config struct {
	S3ProxyAddr string
	KafkaAddr   string
	ChainId     string
	Env         string
	ReorgDeep   int32
}

// NewDevelopmentConfig returns a Dev env Config with default values.
func NewDevelopmentConfig() *Config {
	return &Config{
		S3ProxyAddr: "127.0.0.1:8765",
		KafkaAddr:   "127.0.0.1:9092",
		ChainId:     "256",
		Env:         "test",
		ReorgDeep:   128,
	}
}
