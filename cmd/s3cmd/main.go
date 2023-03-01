package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/DeBankDeFi/db-replicator/pkg/s3"
	"github.com/avast/retry-go"
)

func main() {
	var addr string
	flag.StringVar(&addr, "listen_addr", "127.0.0.1:8765", "listen address")
	flag.Parse()
	s3Client, err := s3.NewClient(addr)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	_, err = s3Client.GetFile(context.Background(), "1")
	if err != nil {
		fmt.Printf("%v\n", err)
	}
	err = retry.Do(
		func() error {
			_, err = s3Client.GetFile(context.Background(), "1")
			if err != nil {
				fmt.Printf("%v\n", err)
				return err
			}
			return nil
		},
		retry.Attempts(10),
		retry.Delay(5*time.Second),
		retry.LastErrorOnly(true),
	)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
}
