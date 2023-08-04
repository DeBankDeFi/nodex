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

package http

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/DeBankDeFi/nodex/pkg/lib/log"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

// Server a wrapped server that handle http request and respond to client.
type Server struct {
	name       string
	bind       string
	httpserver *http.Server
	router     *chi.Mux
}

// NewServer creates a new http server.
func NewServer(_ context.Context, name, addr string) *Server {
	s := &Server{
		name:   name,
		bind:   addr,
		router: chi.NewRouter(),
	}
	return s
}

// Router get server's router to register handler.
func (s *Server) Router() *chi.Mux {
	return s.router
}

// Addr returns the listen address associate with the server.
func (s *Server) Addr() string {
	return s.bind
}

// Serve handles all incoming connections, it blocks util Stop() function invoke explicitly.
func (s *Server) Serve() error {
	if s.httpserver == nil {
		s.httpserver = s.HTTPServer()
	}
	l, err := net.Listen("tcp4", s.bind)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("http server listen to address: %s failed", s.bind))
	}
	s.bind = l.Addr().String()
	log.Info("HTTP server listen and serve", log.Any("server", s.name), log.Any("bind", s.bind))
	return s.httpserver.Serve(l)
}

// HTTPServer returns the underlying HTTP server instance.
func (s *Server) HTTPServer() *http.Server {
	if s.httpserver != nil {
		return s.httpserver
	}
	hs := &http.Server{
		Handler: s.router,
	}
	s.httpserver = hs
	return s.httpserver
}

// Stop stops the underlying HTTP server forcibly.
func (s *Server) Stop() {
	if s.httpserver == nil {
		return
	}

	s.httpserver.Close()
	s.httpserver = nil
}

// GracefulShutdown stops the underlying HTTP server gracefully.
func (s *Server) GracefulShutdown(ctx context.Context) {
	if s.httpserver == nil {
		return
	}
	s.httpserver.Shutdown(ctx)
	log.Info("HTTP server stopped", log.Any("server", s.name))
	s.httpserver = nil
}

// Status implementation of httprouter.Handle interface.
func (s *Server) Status() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" && r.Method != "HEAD" {
			w.WriteHeader(405)
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
}
