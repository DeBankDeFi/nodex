package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/DeBankDeFi/db-replicator/pkg/db"
	"github.com/DeBankDeFi/db-replicator/pkg/db/remote"
	"github.com/DeBankDeFi/db-replicator/pkg/replicator"
	"github.com/DeBankDeFi/db-replicator/pkg/utils"
)

func main() {
	var addr string
	config := &utils.Config{}
	flag.StringVar(&addr, "db_addr", "0.0.0.0:7654", "db listen address")
	flag.StringVar(&config.S3ProxyAddr, "s3proxy_addr", "127.0.0.1:8765", "s3 listen address")
	flag.StringVar(&config.KafkaAddr, "kafka_addr", "127.0.0.1:9092", "kafka listen address")
	flag.StringVar(&config.ChainId, "chain_id", "256", "chain id")
	flag.StringVar(&config.Env, "env", "test", "env")
	flag.IntVar(&config.ReorgDeep, "reorg_deep", 128, "reorg deep")
	flag.Parse()
	errChan := make(chan error)
	stopChan := make(chan os.Signal, 1)

	signal.Notify(stopChan, syscall.SIGTERM, syscall.SIGINT)

	pool := db.NewDBPool()

	reader, err := replicator.NewReader(config, pool, nil)
	if err != nil {
		panic(err)
	}

	err = reader.Start()
	if err != nil {
		panic(err)
	}

	srv, err := remote.NewServer(pool)
	if err != nil {
		panic(err)
	}
	go func() {
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			errChan <- err
		}
		if err = srv.Serve(ln); err != nil {
			errChan <- err
		}
	}()

	defer func() {
		reader.Stop()
		srv.GracefulStop()
	}()

	select {
	case err := <-errChan:
		log.Printf("Fatal error: %v\n", err)
	case <-stopChan:
	}
}
