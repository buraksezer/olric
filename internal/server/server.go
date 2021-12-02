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
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/checkpoint"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/tidwall/redcon"
)

/*
var (
	// CommandsTotal is total number of all requests broken down by command (get, put, etc.) and status.
	CommandsTotal = stats.NewInt64Counter()

	// ConnectionsTotal is total number of connections opened since the server started running.
	ConnectionsTotal = stats.NewInt64Counter()

	// CurrentConnections is current number of open connections.
	CurrentConnections = stats.NewInt64Gauge()

	// WrittenBytesTotal is total number of bytes sent by this server to network.
	WrittenBytesTotal = stats.NewInt64Counter()

	// ReadBytesTotal is total number of bytes read by this server from network.
	ReadBytesTotal = stats.NewInt64Counter()
)
*/

// Config is a composite type to bundle configuration parameters.
type Config struct {
	BindAddr        string
	BindPort        int
	KeepAlivePeriod time.Duration
}

type Server struct {
	config     *Config
	mux        *redcon.ServeMux
	server     *redcon.Server
	log        *flog.Logger
	listener   net.Listener
	StartedCtx context.Context
	started    context.CancelFunc
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	// some components of the TCP server should be closed after the listener
	stopped chan struct{}
}

// New creates and returns a new Server.
func New(c *Config, l *flog.Logger) *Server {
	checkpoint.Add()

	ctx, cancel := context.WithCancel(context.Background())
	startedCtx, started := context.WithCancel(context.Background())
	s := &Server{
		config:     c,
		mux:        redcon.NewServeMux(),
		log:        l,
		started:    started,
		StartedCtx: startedCtx,
		stopped:    make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	return s
}

func (s *Server) ServeMux() *redcon.ServeMux {
	return s.mux
}

// ListenAndServe listens on the TCP network address addr.
func (s *Server) ListenAndServe() error {
	addr := net.JoinHostPort(s.config.BindAddr, strconv.Itoa(s.config.BindPort))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	defer close(s.stopped)
	s.listener = l

	srv := redcon.NewServer(addr,
		s.mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	s.server = srv
	s.started()
	return s.server.Serve(s.listener)
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
// Shutdown works by first closing all open listeners, then closing all idle connections,
// and then waiting indefinitely for connections to return to idle and then shut down.
// If the provided context expires before the shutdown is complete, Shutdown returns
// the context's error, otherwise it returns any error returned from closing the Server's
// underlying Listener(s).
func (s *Server) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	s.cancel()

	var latestError error
	err := s.server.Close()
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to close listener: %v", err)
		latestError = err
	}

	// Listener is closed successfully. Now we can await for closing
	// other components of the TCP server.
	<-s.stopped

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err = ctx.Err()
		if err != nil {
			s.log.V(2).Printf("[ERROR] Context has an error: %v", err)
			latestError = err
		}
	case <-done:
	}

	return latestError
}
