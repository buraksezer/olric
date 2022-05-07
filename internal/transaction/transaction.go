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

package transaction

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-transaction-server/config"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/tidwall/redcon"
	"github.com/tidwall/wal"
	"golang.org/x/sync/errgroup"
)

type Transaction struct {
	config *config.Config
	log    *flog.Logger
	server *server.Server
	wal    *wal.Log
	ctx    context.Context
	cancel context.CancelFunc
}

func New(c *config.Config, lg *log.Logger) (*Transaction, error) {
	fl := flog.New(lg)
	fl.SetLevel(c.Logging.Verbosity)
	if c.Logging.Level == "DEBUG" {
		fl.ShowLineNumber(1)
	}

	w, err := wal.Open(c.OlricTransactionServer.DataDir, nil)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr := &Transaction{
		config: c,
		log:    fl,
		wal:    w,
		ctx:    ctx,
		cancel: cancel,
	}

	keepAlivePeriod, err := time.ParseDuration(c.OlricTransactionServer.KeepAlivePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid KeepAlivePeriod: %v", err)
	}

	rc := &server.Config{
		BindAddr:        c.OlricTransactionServer.BindAddr,
		BindPort:        c.OlricTransactionServer.BindPort,
		KeepAlivePeriod: keepAlivePeriod,
	}
	srv := server.New(rc, fl)
	srv.SetPreConditionFunc(tr.preconditionFunc)
	tr.server = srv
	tr.registerHandlers()

	return tr, nil
}

func (tr *Transaction) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	// TODO: ???
	return true
}

func (tr *Transaction) registerHandlers() {
	tr.server.ServeMux().HandleFunc(TransactionCmd.Resolve, tr.transactionResolveHandler)
}

func (tr *Transaction) Start() error {
	tr.log.V(1).Printf("[INFO] olric-transaction-server %s on %s/%s %s", olric.ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return tr.server.ListenAndServe()
	})

	select {
	case <-tr.server.StartedCtx.Done():
		// TCP server has been started
		tr.log.V(2).Printf("[INFO] Transaction server bindAddr: %s, bindPort: %d",
			tr.config.OlricTransactionServer.BindAddr, tr.config.OlricTransactionServer.BindPort)
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Shutdown here because we could not start anything.
		return errGr.Wait()
	}

	// Wait for the TCP server.
	return errGr.Wait()
}

func (tr *Transaction) Shutdown(ctx context.Context) error {
	select {
	case <-tr.ctx.Done():
		// Shutdown only once.
		return nil
	default:
	}

	tr.cancel()

	var latestErr error
	// Shutdown RESP server
	if err := tr.server.Shutdown(ctx); err != nil {
		tr.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestErr = err
	}

	if err := tr.wal.Close(); err != nil {
		tr.log.V(2).Printf("[ERROR] Failed to close WAL store: %v", err)
		latestErr = err
	}
	return latestErr
}
