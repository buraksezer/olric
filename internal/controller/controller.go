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

package controller

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/tidwall/redcon"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/cmd/olric-control-server/config"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/cockroachdb/pebble"
	"golang.org/x/sync/errgroup"
)

var OlricLatestVersionKey = []byte("OlricLatestVersion")

type Controller struct {
	mtx            sync.RWMutex
	currentVersion uint32
	config         *config.Config
	log            *flog.Logger
	server         *server.Server
	pebble         *pebble.DB
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func New(c *config.Config, lg *log.Logger) (*Controller, error) {
	fl := flog.New(lg)
	fl.SetLevel(c.Logging.Verbosity)
	if c.Logging.Level == "DEBUG" {
		fl.ShowLineNumber(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctr := &Controller{
		config: c,
		log:    fl,
		ctx:    ctx,
		cancel: cancel,
	}

	keepAlivePeriod, err := time.ParseDuration(c.OlricControlServer.KeepAlivePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid KeepAlivePeriod: %v", err)
	}

	pb, err := pebble.Open(c.OlricControlServer.DataDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("unable to open pebble database: %v", err)
	}
	ctr.pebble = pb

	err = ctr.loadCurrentVersionFromPebble()
	if err != nil {
		return nil, err
	}

	rc := &server.Config{
		BindAddr:        c.OlricControlServer.BindAddr,
		BindPort:        c.OlricControlServer.BindPort,
		KeepAlivePeriod: keepAlivePeriod,
	}
	srv := server.New(rc, fl)
	srv.SetPreConditionFunc(ctr.preconditionFunc)
	ctr.server = srv
	ctr.registerHandlers()

	return ctr, nil
}

func (c *Controller) loadCurrentVersionFromPebble() error {
	value, closer, err := c.pebble.Get(OlricLatestVersionKey)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to load %s from Pebble database: %v", string(OlricLatestVersionKey), err)
	}
	defer func() {
		if err = closer.Close(); err != nil {
			c.log.V(2).Printf("[ERROR] Failed to call close on %s on Pebble: %v", string(OlricLatestVersionKey), err)
		}
	}()
	c.currentVersion = binary.BigEndian.Uint32(value)
	return nil
}

func (c *Controller) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	// TODO: ???
	return true
}

func (c *Controller) registerHandlers() {
	c.server.ServeMux().HandleFunc(Cluster.CommitVersion, c.clusterCommitVersionHandler)
	c.server.ServeMux().HandleFunc(Cluster.ReadVersion, c.clusterReadVersionHandler)
}

func (c *Controller) Start() error {
	c.log.V(1).Printf("[INFO] olric-control-server %s on %s/%s %s", olric.ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return c.server.ListenAndServe()
	})

	select {
	case <-c.server.StartedCtx.Done():
		// TCP server has been started
		c.log.V(2).Printf("[INFO] Control server bindAddr: %s, bindPort: %d",
			c.config.OlricControlServer.BindAddr, c.config.OlricControlServer.BindPort)
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Shutdown here because we could not start anything.
		return errGr.Wait()
	}

	// Wait for the TCP server.
	return errGr.Wait()
}

func (c *Controller) Shutdown(ctx context.Context) error {
	select {
	case <-c.ctx.Done():
		// Shutdown only once.
		return nil
	default:
	}

	c.cancel()

	var latestErr error
	// Shutdown Redcon server
	if err := c.server.Shutdown(ctx); err != nil {
		c.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestErr = err
	}

	if err := c.pebble.Close(); err != nil {
		c.log.V(2).Printf("[ERROR] Failed to close Pebble database: %v", err)
		latestErr = err
	}
	return latestErr
}
