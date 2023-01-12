package utils

// Config represents the configuration of the replicator.
type Config struct {
	S3ProxyAddr      string
	KafkaAddr        string
	RemoteAddr       string
	RemoteListenAddr string
	ChainId          string
	Env              string
	ReorgDeep        int
	DBCacheSize      int
}

// NewDevelopmentConfig returns a Dev env Config with default values.
func NewDevelopmentConfig() *Config {
	return &Config{
		S3ProxyAddr:      "127.0.0.1:8765",
		KafkaAddr:        "127.0.0.1:9092",
		RemoteAddr:       "127.0.0.1:7654",
		RemoteListenAddr: "0.0.0.0:7654",
		ChainId:          "256",
		Env:              "test",
		ReorgDeep:        128,
		DBCacheSize:      1 << 32,
	}
}
