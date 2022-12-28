package main

import (
	"flag"

	"github.com/DeBankDeFi/db-replicator/pkg/s3"
)

func main() {
	var addr string
	flag.StringVar(&addr, "addr", "0.0.0.0:8765", "listen address")
	flag.Parse()
	err := s3.ListenAndServe(addr)
	if err != nil {
		panic(err)
	}
}
