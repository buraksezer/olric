// Copyright 2018-2022 Burak Sezer
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

package resolver

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/process"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/resolver/config"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/tidwall/redcon"
	"golang.org/x/sync/errgroup"
)

func registerErrors() {
	protocol.SetError("TRANSACTIONABORT", ErrTransactionAbort)
}

type Resolver struct {
	config *config.Config
	log    *flog.Logger
	server *server.Server
	ssi    *SSI
	ctx    context.Context
	cancel context.CancelFunc
}

func (r *Resolver) Name() string {
	return "resolver"
}

func (r *Resolver) Logger() *flog.Logger {
	return r.log
}

func New(c *config.Config, lg *log.Logger) (*Resolver, error) {
	fl := flog.New(lg)
	fl.SetLevel(c.Logging.Verbosity)
	if c.Logging.Level == "DEBUG" {
		fl.ShowLineNumber(1)
	}

	keepAlivePeriod, err := time.ParseDuration(c.OlricResolver.KeepAlivePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid KeepAlivePeriod: %v", err)
	}

	s := NewSSI(5*time.Second, 15*time.Second)
	s.Start()

	ctx, cancel := context.WithCancel(context.Background())
	r := &Resolver{
		config: c,
		log:    fl,
		ssi:    s,
		ctx:    ctx,
		cancel: cancel,
	}

	rc := &server.Config{
		BindAddr:        c.OlricResolver.BindAddr,
		BindPort:        c.OlricResolver.BindPort,
		KeepAlivePeriod: keepAlivePeriod,
	}
	srv := server.New(rc, fl)
	srv.SetPreConditionFunc(r.preconditionFunc)
	r.server = srv
	r.registerHandlers()
	registerErrors()

	return r, nil
}

func (r *Resolver) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	// TODO: ???
	return true
}

func (r *Resolver) registerHandlers() {
	r.server.ServeMux().HandleFunc(protocol.Generic.Ping, r.pingCommandHandler)
	r.server.ServeMux().HandleFunc(protocol.Resolver.Commit, r.commitCommandHandler)
}

func (r *Resolver) Start() error {
	r.log.V(1).Printf("[INFO] olric-resolver %s on %s/%s %s", olric.ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return r.server.ListenAndServe()
	})

	select {
	case <-r.server.StartedCtx.Done():
		// TCP server has been started
		r.log.V(2).Printf("[INFO] Resolver bindAddr: %s, bindPort: %d",
			r.config.OlricResolver.BindAddr, r.config.OlricResolver.BindPort)
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Stop here because we could not start anything.
		return errGr.Wait()
	}

	// Wait for the TCP server.
	return errGr.Wait()
}

func (r *Resolver) Shutdown(ctx context.Context) error {
	select {
	case <-r.ctx.Done():
		// Stop only once.
		return nil
	default:
	}

	r.cancel()

	r.ssi.Stop()

	var latestErr error
	// Stop Redcon server
	if err := r.server.Shutdown(ctx); err != nil {
		r.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestErr = err
	}

	return latestErr
}

var _ process.Service = (*Resolver)(nil)
