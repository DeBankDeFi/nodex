package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/reader"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
)

func main() {
	config := &utils.Config{}
	flag.StringVar(&config.RemoteListenAddr, "listen_addr", "0.0.0.0:7654", "remote server listen address")
	flag.StringVar(&config.S3ProxyAddr, "s3proxy_addr", "127.0.0.1:8765", "s3 address")
	flag.StringVar(&config.KafkaAddr, "kafka_addr", "127.0.0.1:9092", "kafka address")
	flag.StringVar(&config.ChainId, "chain_id", "256", "chain id")
	flag.StringVar(&config.Env, "env", "test", "env")
	flag.IntVar(&config.ReorgDeep, "reorg_deep", 128, "chain reorg deep")
	flag.Parse()
	stopChan := make(chan os.Signal, 1)

	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	pool := db.NewDBPool()

	reader, err := reader.NewReader(config, pool, nil)
	if err != nil {
		panic(err)
	}

	err = reader.Start()
	if err != nil {
		panic(err)
	}

	defer func() {
		reader.Stop()
	}()
	<-stopChan
}