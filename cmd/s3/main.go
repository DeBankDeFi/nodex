package main

import (
	"flag"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DeBankDeFi/db-replicator/pkg/s3"
)

func main() {
	var addr string
	var prometheusAddr string
	var cacheSize int
	flag.StringVar(&addr, "listen_addr", "0.0.0.0:8765", "listen address")
	flag.StringVar(&prometheusAddr, "metric_address", ":10086", "metric address")
	flag.IntVar(&cacheSize, "cache_size", 32, "cache size")
	flag.Parse()
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("pong"))
		})
		http.ListenAndServe(prometheusAddr, nil)
	}()
	err := s3.ListenAndServe(addr, uint(cacheSize))
	if err != nil {
		panic(err)
	}
}
