//go:build !unittest
// +build !unittest

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

package http_test

import (
	"bytes"
	"context"
	"io/ioutil"
	stdhttp "net/http"
	"testing"
	"time"

	"github.com/DeBankDeFi/nodex/pkg/internal/server/http"
)

func newServer() *http.Server {
	s := http.NewServer(context.Background(), "test", ":8899")
	go s.Serve()
	return s
}

func TestNewServer(t *testing.T) {
	s := newServer()
	defer s.GracefulShutdown(context.Background())

	<-time.After(500 * time.Millisecond) // waiting server has been started
	resp, err := stdhttp.Get("http://localhost:8899/status")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	expected := []byte(`{"status":"ok"}`)
	if !bytes.Equal(body, expected) {
		t.Logf("HTTP server returned an unexpected response, expected: %s, actual: %s", string(body), string(expected))
		t.FailNow()
	}
}
