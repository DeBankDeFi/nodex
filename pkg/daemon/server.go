// Copyright (c) 2022 DeBank Inc. <admin@debank.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package daemon

import (
	"context"
	"net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/DeBankDeFi/nodex/pkg/internal/server/grpc"
	"github.com/DeBankDeFi/nodex/pkg/internal/server/http"
	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/DeBankDeFi/nodex/pkg/types"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

type closer interface {
	// GracefulShutdown close underlying server gracefully.
	GracefulShutdown(ctx context.Context)
}

// Daemon server behaviour abstraction.
type Daemon interface {
	context.Context

	// Start wrapped server, server will block util an error occurs or
	// invoking Shutdown() explicitly.
	Start()
	// Shutdown close all servers under the hood.
	Shutdown()
}

type server struct {
	context.Context
	cancel   context.CancelFunc
	cfg      *types.DaemonFlag
	s3Client *s3.Client
	err      error
	shutting bool
	done     chan struct{}
	closer   []closer
}

// NewDaemon creates a new daemon server that can handle incoming requests with grpc or http protocol.
func NewDaemon(ctx context.Context, cfg *types.DaemonFlag) (Daemon, error) {
	var s3Client *s3.Client
	if cfg.UseS3 {
		sdkConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(cfg.S3Region))
		if err != nil {
			return nil, err
		}
		s3Client = s3.NewFromConfig(sdkConfig)
	}
	ctx, cancel := context.WithCancel(ctx)
	s := &server{
		Context:  ctx,
		cancel:   cancel,
		cfg:      cfg,
		done:     make(chan struct{}, 1),
		closer:   make([]closer, 0),
		s3Client: s3Client,
	}

	return s, nil
}

// Start starts the server.
func (s *server) Start() {
	g, ctx := errgroup.WithContext(s.Context)

	g.Go(func() error {
		return s.startGrpcServer(ctx)
	})

	g.Go(func() error {
		return s.startHTTPServer(ctx)
	})

	go func() {
		// record waiting error
		s.err = g.Wait()
		s.done <- struct{}{}
		if !s.shutting {
			s.Shutdown()
		}
	}()

	s.handleSysSignal()
}

func (s *server) startHTTPServer(ctx context.Context) error {
	srv := http.NewServer(ctx, "http-api", s.cfg.HTTPListen)

	r := srv.Router()
	r.Use(middleware.RealIP)
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.NoCache)
	r.Handle("/metrics", promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, promhttp.HandlerFor(
			prometheus.DefaultGatherer,
			promhttp.HandlerOpts{MaxRequestsInFlight: 1024},
		),
	))
	r.Handle("/ping", srv.Status())
	if s.cfg.EnablePprof {
		r.HandleFunc("/debug/pprof/*", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)

		r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		r.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		r.Handle("/debug/pprof/block", pprof.Handler("block"))
		r.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	}

	s.registerCloser(srv)

	return srv.Serve()
}

func (s *server) startGrpcServer(ctx context.Context) error {
	srv := grpc.NewServer(ctx, s.cfg.GRPCListen)

	// IMPORTANT! register all rpc services to grpc server.
	registerServices(srv, s.s3Client, s.cfg.BucketName, s.cfg.Prefix)

	s.registerCloser(srv)

	return srv.Serve()
}

func (s *server) Shutdown() {
	s.shutting = true
	var wg sync.WaitGroup
	wg.Add(len(s.closer))
	for _, c := range s.closer {
		go func(c closer) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c.GracefulShutdown(ctx)
		}(c)
	}
	wg.Wait()
	s.shutdown()
}

func (s *server) shutdown() {
	s.cancel()
	select {
	case <-s.done:
		if s.err != nil {
			log.Error("server shutdown completed along with an error", s.err)
			return
		}
	case <-time.After(3 * time.Second):
		log.Warn("timeout exceed while shutting-down server gracefully")
		return
	}
}

func (s *server) registerCloser(svcs ...closer) {
	if s.closer == nil {
		s.closer = make([]closer, 0)
	}
	s.closer = append(s.closer, svcs...)
}

func (s *server) handleSysSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	for {
		select {
		case sig := <-sigCh:
			log.Warn("System signal captured, node will shutdown", log.Any("signal", sig.String()))
			s.Shutdown()
			return
		case <-s.Context.Done():
			log.Warn("Context canceled by main process, node will shutdown", log.Any("error", s.Context.Err()))
			s.Shutdown()
			return
		case <-s.done:
			if s.err != nil {
				log.Error("Program error encountered during node running", s.err)
			}
			s.Shutdown()
			return
		}
	}
}
