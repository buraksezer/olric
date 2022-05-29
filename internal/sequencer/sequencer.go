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

package sequencer

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/process"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/sequencer/config"
	"github.com/buraksezer/olric/internal/server"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/cockroachdb/pebble"
	"github.com/tidwall/redcon"
	"golang.org/x/sync/errgroup"
)

var ReadVersionKey = []byte("ReadVersion")

type Sequencer struct {
	mtx           sync.RWMutex
	readVersion   int64
	commitVersion int64
	config        *config.Config
	log           *flog.Logger
	server        *server.Server
	pebble        *pebble.DB
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

func New(c *config.Config, lg *log.Logger) (*Sequencer, error) {
	fl := flog.New(lg)
	fl.SetLevel(c.Logging.Verbosity)
	if c.Logging.Level == "DEBUG" {
		fl.ShowLineNumber(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctr := &Sequencer{
		config: c,
		log:    fl,
		ctx:    ctx,
		cancel: cancel,
	}

	keepAlivePeriod, err := time.ParseDuration(c.OlricSequencer.KeepAlivePeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid KeepAlivePeriod: %v", err)
	}

	pb, err := pebble.Open(c.OlricSequencer.DataDir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("unable to open pebble database: %v", err)
	}
	ctr.pebble = pb

	rc := &server.Config{
		BindAddr:        c.OlricSequencer.BindAddr,
		BindPort:        c.OlricSequencer.BindPort,
		KeepAlivePeriod: keepAlivePeriod,
	}
	srv := server.New(rc, fl)
	srv.SetPreConditionFunc(ctr.preconditionFunc)
	ctr.server = srv
	ctr.registerHandlers()

	return ctr, nil
}

func (s *Sequencer) Name() string {
	return "sequencer"
}

func (s *Sequencer) Logger() *flog.Logger {
	return s.log
}

func (s *Sequencer) advanceCommitVersion() {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	s.commitVersion = time.Now().Unix() * 1000000
}

func (s *Sequencer) advanceCommitVersionAtBackground() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	s.advanceCommitVersion()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.advanceCommitVersion()
		}
	}
}

func (s *Sequencer) loadReadVersionFromPebble() error {
	value, closer, err := s.pebble.Get(ReadVersionKey)
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("unable to load %s from Pebble database: %v", string(ReadVersionKey), err)
	}
	defer func() {
		if err = closer.Close(); err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call close on %s on Pebble: %v", string(ReadVersionKey), err)
		}
	}()
	s.readVersion = int64(binary.BigEndian.Uint64(value))
	return nil
}

func (s *Sequencer) preconditionFunc(conn redcon.Conn, _ redcon.Command) bool {
	// TODO: ???
	return true
}

func (s *Sequencer) registerHandlers() {
	s.server.ServeMux().HandleFunc(protocol.Sequencer.GetCommitVersion, s.getCommitVersionHandler)
	s.server.ServeMux().HandleFunc(protocol.Sequencer.GetReadVersion, s.getReadVersionHandler)
	s.server.ServeMux().HandleFunc(protocol.Sequencer.UpdateReadVersion, s.updateReadVersionHandler)
	s.server.ServeMux().HandleFunc(protocol.Generic.Ping, s.pingCommandHandler)
}

func (s *Sequencer) Start() error {
	err := s.loadReadVersionFromPebble()
	if err != nil {
		return err
	}

	s.log.V(1).Printf("[INFO] olric-sequencer %s on %s/%s %s", olric.ReleaseVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())

	// This error group is responsible to run the TCP server at background and report errors.
	errGr, ctx := errgroup.WithContext(context.Background())
	errGr.Go(func() error {
		return s.server.ListenAndServe()
	})

	select {
	case <-s.server.StartedCtx.Done():
		// TCP server has been started
		s.log.V(2).Printf("[INFO] Sequencer bindAddr: %s, bindPort: %d",
			s.config.OlricSequencer.BindAddr, s.config.OlricSequencer.BindPort)
	case <-ctx.Done():
		// TCP server could not be started due to an error. There is no need to run
		// Olric.Stop here because we could not start anything.
		return errGr.Wait()
	}

	s.wg.Add(1)
	go s.advanceCommitVersionAtBackground()

	// Wait for the TCP server.
	return errGr.Wait()
}

func (s *Sequencer) Shutdown(ctx context.Context) error {
	select {
	case <-s.ctx.Done():
		// Stop only once.
		return nil
	default:
	}

	s.cancel()
	s.wg.Wait()

	var latestErr error
	// Stop Redcon server
	if err := s.server.Shutdown(ctx); err != nil {
		s.log.V(2).Printf("[ERROR] Failed to shutdown RESP server: %v", err)
		latestErr = err
	}

	if err := s.pebble.Close(); err != nil {
		s.log.V(2).Printf("[ERROR] Failed to close Pebble database: %v", err)
		latestErr = err
	}
	return latestErr
}

var _ process.Service = (*Sequencer)(nil)
