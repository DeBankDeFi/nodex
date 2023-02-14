package utils

import (
	"encoding/json"
	"os"

	"github.com/DeBankDeFi/db-replicator/pkg/pb"
)

// Config represents the configuration of the replicator.
type Config struct {
	S3ProxyAddr      string
	KafkaAddr        string
	RemoteAddr       string
	RemoteListenAddr string
	Env              string
	ChainId          string
	Role             string
	DBInfoPath       string
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
		Env:              "test",
		ChainId:          "256",
		Role:             "master",
		DBInfoPath:       "dbinfo.json",
		ReorgDeep:        128,
		DBCacheSize:      1 << 32,
	}
}

func OpenAndReadDbInfo(path string) (*pb.DBInfoList, error) {
	dbInfo := &pb.DBInfoList{}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	err = json.NewDecoder(f).Decode(dbInfo)
	if err != nil {
		return nil, err
	}
	return dbInfo, nil
}
