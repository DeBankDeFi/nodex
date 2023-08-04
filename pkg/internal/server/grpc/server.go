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

package grpc

import (
	"context"
	"net"
	"runtime/debug"
	"sync"

	"github.com/DeBankDeFi/nodex/pkg/lib/log"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server wrapper the grpc server.
type Server struct {
	bind string
	rs   *grpc.Server

	mu    sync.Mutex
	gOpts []grpc.ServerOption
}

// NewServer creates a wrapped grpc server instance.
func NewServer(ctx context.Context, listenAddr string) *Server {
	s := &Server{
		bind:  listenAddr,
		gOpts: make([]grpc.ServerOption, 0),
	}

	s.initInterceptors()

	return s
}

func (s *Server) initInterceptors() {
	s.AppendGRPCInterceptors(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			grpc_ctxtags.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
				return status.Errorf(codes.Internal, "%s: %s", p, debug.Stack())
			})),
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(func(p interface{}) (err error) {
				return status.Errorf(codes.Internal, "%s: %s", p, debug.Stack())
			})),
		)),
	)
}

// AppendGRPCInterceptors appending interceptors as grpc server options.
func (s *Server) AppendGRPCInterceptors(opts ...grpc.ServerOption) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gOpts = append(s.gOpts, opts...)
}

// GrpcServer returns the underlying grpc server.
func (s *Server) GrpcServer() *grpc.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rs != nil {
		return s.rs
	}

	rs := grpc.NewServer(s.gOpts...)
	s.rs = rs
	return s.rs
}

// Addr returns listen address.
func (s *Server) Addr() string {
	return s.bind
}

// Serve handles incoming grpc connections, it blocks util invoking Stop or GracefulStop method.
func (s *Server) Serve() error {
	if s.rs == nil {
		s.rs = s.GrpcServer()
	}

	l, err := net.Listen("tcp4", s.bind)
	if err != nil {
		return err
	}
	// rewrite s.bind
	s.bind = l.Addr().String()

	log.Info("grpc server listening on", zap.Any("address", s.bind))
	return s.rs.Serve(l)
}

// Stop close the underlying grpc server.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rs == nil {
		return
	}
	s.rs.Stop()
}

// GracefulShutdown close the underlying grpc server gracefully.
func (s *Server) GracefulShutdown(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rs == nil {
		return
	}
	log.Info("Grpc server stopped")
	s.rs.Stop()
}
