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

/*package http provides control and configuration mechanisms on top of Golang's HTTP server implementation*/
package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/sync/errgroup"
)

// Currently Olric only supports HTTP
const scheme = "http"

// Server represents an HTTP server which can be configured and controlled easily in Olric.
type Server struct {
	config     *config.Http
	log        *flog.Logger
	srv        *http.Server
	ctx        context.Context
	cancel     context.CancelFunc
	StartedCtx context.Context
	started    context.CancelFunc
}

// New returnes a new HTTP server instance.
func New(c *config.Http, log *flog.Logger, router *httprouter.Router) *Server {
	// Check aliveness firstly, we don't want to accept connections until the HTTP server works without any problem.
	//
	// We register the aliveness handler here because this package is indepentend from the top-level Olric package and
	// this handler is useful for the function of this package and the tests.
	router.HandlerFunc("GET", "/api/v1/system/aliveness", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	srv := &http.Server{
		Addr:    c.Addr,
		Handler: router,
	}
	startedCtx, started := context.WithCancel(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		config:     c,
		log:        log,
		srv:        srv,
		ctx:        ctx,
		cancel:     cancel,
		StartedCtx: startedCtx,
		started:    started,
	}
}

// alivenessProbe checks the aliveness endpoint periodically and returns nil if it returns 204.
func (s *Server) alivenessProbe() error {
	alivenessURL := fmt.Sprintf("%s://%s/api/v1/system/aliveness", scheme, s.srv.Addr)
	req, err := http.NewRequestWithContext(s.ctx, "GET", alivenessURL, nil)
	if err != nil {
		return err
	}

	s.log.V(2).Printf("[INFO] Awaiting for HTTP server start")
	for i := 0; i < 10; i++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to do an HTTP request to: %s: %v", alivenessURL, err)
			return err
		}
		s.log.V(6).Printf("[DEBUG] HTTP server returned %d to aliveness check", resp.StatusCode)
		if resp.StatusCode == http.StatusNoContent {
			s.started()
			// Now, the server works. We ready to accept connections.
			s.log.V(2).Printf("[INFO] HTTP server addr: %s", s.config.Addr)
			return nil
		}
		<-time.After(time.Second)
	}
	return fmt.Errorf("failed to start a new HTTP server")
}

// Start starts a new HTTP server. Server.StartedCtx is cancelled when the server is ready to accept connections.
func (s *Server) Start() error {
	g, ctx := errgroup.WithContext(context.Background())

	// We need this to cancel StartedCtx. Basically it's a callback.
	g.Go(func() error {
		return s.alivenessProbe()
	})

	g.Go(func() error {
		err := s.srv.ListenAndServe()
		if err == http.ErrServerClosed {
			s.log.V(6).Printf("[DEBUG] HTTP server closed without an error")
			err = nil
		}
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to start HTTP server: %v", err)
		}
		return err
	})

	select {
	case <-s.StartedCtx.Done():
	case <-ctx.Done():
		// Something went wrong in starting procedure.
		_ = s.Shutdown(context.Background())
		return g.Wait()
	}

	// Wait for shutdown or an error.
	return g.Wait()
}

// Shutdown calls Shutdown method of Golang's HTTP server and closes underyling data structures.
func (s *Server) Shutdown(ctx context.Context) error {
	defer s.cancel()

	err := s.srv.Shutdown(ctx)
	if err != nil {
		// Error from closing listeners, or context timeout:
		s.log.V(2).Printf("[ERROR] Failed to call HTTP server Shutdown: %v", err)
	}
	return err
}
