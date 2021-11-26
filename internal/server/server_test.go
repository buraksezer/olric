// Copyright 2018-2021 Burak Sezer
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

package server

import (
	"context"
	"crypto/rand"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// getFreePort copied from testutil package to prevent cycle import.
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

func newServer(t *testing.T) *Server {
	bindPort, err := getFreePort()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	l := log.New(os.Stdout, "server-test: ", log.LstdFlags)
	fl := flog.New(l)
	fl.SetLevel(6)
	fl.ShowLineNumber(1)
	c := &Config{
		BindAddr:        "127.0.0.1",
		BindPort:        bindPort,
		KeepAlivePeriod: time.Second,
	}
	s := New(c, fl)

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	t.Cleanup(func() {
		err = s.Shutdown(context.Background())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	return s
}

func defaultRedisOptions(c *Config) *redis.Options {
	return &redis.Options{
		Addr: net.JoinHostPort(c.BindAddr, strconv.Itoa(c.BindPort)),
	}
}

func TestServer_RESP(t *testing.T) {
	s := newServer(t)
	defer func() {
		require.NoError(t, s.Shutdown(context.Background()))
	}()

	data := make([]byte, 8)
	_, err := rand.Read(data)
	require.NoError(t, err)

	s.ServeMux().HandleFunc(resp.GET, func(conn redcon.Conn, cmd redcon.Command) {
		conn.WriteBulk(data)
	})

	<-s.StartedCtx.Done()

	rdb := redis.NewClient(defaultRedisOptions(s.config))

	ctx := context.Background()
	cmd := resp.Get(ctx, "mydmap", "mykey")
	err = rdb.Process(ctx, cmd)
	require.NoError(t, err)

	result, err := cmd.Bytes()
	require.NoError(t, err)
	require.Equal(t, data, result)
}
