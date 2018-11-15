// Copyright 2018 Burak Sezer
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

/*Package server provides a standalone server implementation for Olric*/
package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/buraksezer/olric"
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

// Olricd represents a new Olricd instance.
type Olricd struct {
	logger *log.Logger
	config *olric.Config
	db     *olric.Olric
	errgr  errgroup.Group
}

// New creates a new Server instance
func New(c *Config) (*Olricd, error) {
	s := &Olricd{}
	var logDest io.Writer
	if c.Logging.Output == "stderr" {
		logDest = os.Stderr
	} else if c.Logging.Output == "stdout" {
		logDest = os.Stdout
	} else {
		logDest = os.Stderr
	}

	if c.Logging.Level == "" {
		c.Logging.Level = olric.DefaultLogLevel
	}

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.Logging.Level)),
		Writer:   logDest,
	}
	s.logger = log.New(logDest, "", log.LstdFlags)
	s.logger.SetOutput(filter)

	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var serializer olric.Serializer
	if c.Olricd.Serializer == "json" {
		serializer = olric.NewJSONSerializer()
	} else if c.Olricd.Serializer == "msgpack" {
		serializer = olric.NewMsgpackSerializer()
	} else if c.Olricd.Serializer == "gob" {
		serializer = olric.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", c.Olricd.Serializer)
	}

	mc, err := newMemberlistConf(c)
	if err != nil {
		return nil, err
	}

	var keepAlivePeriod time.Duration
	if c.Olricd.KeepAlivePeriod != "" {
		keepAlivePeriod, err = time.ParseDuration(c.Olricd.KeepAlivePeriod)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.keepAlivePeriod: '%s'", c.Olricd.KeepAlivePeriod))
		}
	}
	s.config = &olric.Config{
		Name:             c.Olricd.Name,
		MemberlistConfig: mc,
		KeyFile:          c.Olricd.KeyFile,
		CertFile:         c.Olricd.CertFile,
		LogLevel:         c.Logging.Level,
		Peers:            c.Memberlist.Peers,
		PartitionCount:   c.Olricd.PartitionCount,
		BackupCount:      c.Olricd.BackupCount,
		BackupMode:       c.Olricd.BackupMode,
		LoadFactor:       c.Olricd.LoadFactor,
		Logger:           s.logger,
		Hasher:           olric.NewDefaultHasher(),
		Serializer:       serializer,
		KeepAlivePeriod:  keepAlivePeriod,
		MaxValueSize:     c.Olricd.MaxValueSize,
	}
	if c.Snapshot.Enabled {
		s.config.OperationMode = olric.OpInMemoryWithSnapshot
		// Check data dir on disk.
		err = unix.Access(c.Snapshot.Dir, unix.W_OK)
		if err != nil {
			// TODO: We may want to create it if it doesn't exist on disk.
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse snapshot.dir: '%s'", c.Snapshot.Dir))
		}

		// Parse config params to control snapshot package.
		s.config.GCDiscardRatio = c.Snapshot.GCDiscardRatio
		if len(c.Snapshot.GCInterval) != 0 {
			gcInterval, err := time.ParseDuration(c.Snapshot.GCInterval)
			if err != nil {
				return nil, errors.WithMessage(err,
					fmt.Sprintf("failed to parse snapshot.gcInterval: '%s'", c.Snapshot.GCInterval))
			}
			s.config.GCInterval = gcInterval
		}
		if len(c.Snapshot.Interval) != 0 {
			interval, err := time.ParseDuration(c.Snapshot.Interval)
			if err != nil {
				return nil, errors.WithMessage(err,
					fmt.Sprintf("failed to parse snapshot.interval: '%s'", c.Snapshot.Interval))
			}
			s.config.SnapshotInterval = interval
		}
		opt := &badger.DefaultOptions
		opt.Dir = c.Snapshot.Dir
		opt.ValueDir = c.Snapshot.Dir
		s.config.BadgerOptions = opt
	}
	return s, nil
}

func (s *Olricd) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	s.logger.Printf("[INFO] Signal catched: %s", ch.String())
	s.errgr.Go(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.db.Shutdown(ctx); err != nil {
			s.logger.Printf("[ERROR] Failed to shutdown Olric: %v", err)
			return err
		}
		return nil
	})
}

// Start starts a new olricd server instance and blocks until the server is closed.
func (s *Olricd) Start() error {
	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	db, err := olric.New(s.config)
	if err != nil {
		return err
	}
	s.db = db
	s.logger.Printf("[INFO] olricd (pid: %d) has been started on %s", os.Getpid(), s.config.Name)
	s.errgr.Go(func() error {
		if err = s.db.Start(); err != nil {
			s.logger.Printf("[ERROR] Failed to run Olric: %v", err)
			return err
		}
		return nil
	})
	return s.errgr.Wait()
}
