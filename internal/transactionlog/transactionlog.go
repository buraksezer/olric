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

package transactionlog

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/process"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/internal/transactionlog/config"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/tidwall/redcon"
	"golang.org/x/sync/errgroup"
)

func registerErrors() {
}

type TransactionLog struct {
	config *config.Config
	log    *flog.Logger
	server *server.Server
	ctx    context.Context
	cancel context.CancelFunc
}

func (t *TransactionLog) Name() string {
	return "resolver"
}

func (t *TransactionLog) Logger() *flog.Logger {
	return t.log
}

func New(c *config.Config, lg *log.Logger) (*TransactionLog, error) {
	fl := flog.New(lg)
	fl.SetLevel(c.Logging.Verbosity)
	if c.Logging.Level == "DEBUG" {
		fl.ShowLineNumber(1)
	}

	keepAlivePeriod, err := time.ParseDuration(c.OlricTransactionLog.KeepAlivePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid KeepAlivePeriod: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	tl := &TransactionLog{
		config: c,
		log:    fl,
		ctx:    ctx,
		cancel: cancel,
	}

	rc := &server.Config{
		BindAddr:        c.OlricTransactionLog.BindAddr,
		BindPort:        c.OlricTransactionLog.BindPort,
		KeepAlivePeriod: keepAlivePeriod,
	}
	srv := server.New(rc, fl)
	srv.SetPreConditionFunc(tl.preconditionFunc)
	tl.server = srv
	tl.registerHandlers()
	registerErrors()

	return tl, nil
}

func (t *TransactionLog) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	// TODO: ???
	return true
}

func (t *TransactionLog) registerHandlers() {
	t.server.ServeMux().HandleFunc(protocol.Generic.Ping, t.pingCommandHandler)
	//t.server.ServeMux().HandleFunc(protocol.Resolver.Commit, t.commitCommandHandler)
}

func (t *TransactionLog) Start() error {
	t.log.V(1).Printf("[INFO] olric-transaction-log %s on %s/%s %s", olric.ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return t.server.ListenAndServe()
	})

	select {
	case <-t.server.StartedCtx.Done():
		// TCP server has been started
		t.log.V(2).Printf("[INFO] Transaction Log bindAddr: %s, bindPort: %d",
			t.config.OlricTransactionLog.BindAddr, t.config.OlricTransactionLog.BindPort)
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Stop here because we could not start anything.
		return errGr.Wait()
	}

	// Wait for the TCP server.
	return errGr.Wait()
}

func (t *TransactionLog) Shutdown(ctx context.Context) error {
	select {
	case <-t.ctx.Done():
		// Stop only once.
		return nil
	default:
	}

	t.cancel()

	var latestErr error
	// Stop Redcon server
	if err := t.server.Shutdown(ctx); err != nil {
		t.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestErr = err
	}

	return latestErr
}

var _ process.Service = (*TransactionLog)(nil)
