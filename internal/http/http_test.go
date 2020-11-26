// Copyright 2018-2020 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/sync/errgroup"
)

func getRandomAddr() (string, int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return "", 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", 0, err
	}
	defer l.Close()
	laddr := l.Addr().String()
	bindAddr, rawPort, err := net.SplitHostPort(laddr)
	if err != nil {
		return "", 0, err
	}
	bindPort, err := strconv.Atoi(rawPort)
	if err != nil {
		return "", 0, err
	}
	return bindAddr, bindPort, nil
}

func TestHTTP_Start(t *testing.T) {
	bindAddr, bindPort, err := getRandomAddr()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	c := &config.HTTPConfig{
		Enabled:  true,
		BindAddr: bindAddr,
		BindPort: bindPort,
	}

	// Set a simple logger
	logger := flog.New(log.New(os.Stderr, "", log.LstdFlags))
	logger.SetLevel(6)
	logger.ShowLineNumber(1)
	router := httprouter.New()

	// Create a new HTTP server
	srv := New(c, logger, router)

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return srv.Start()
	})

	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	case <-srv.StartedCtx.Done():
	case <-time.After(10 * time.Second):
		t.Errorf("Failed to start a new HTTP server")
	}

	err = srv.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = g.Wait()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
