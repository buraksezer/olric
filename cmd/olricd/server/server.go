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

/*Package server provides a standalone server implementation for OlricDB*/
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

	"github.com/buraksezer/olricdb"
	"github.com/hashicorp/logutils"
	"github.com/pkg/errors"
)

// Olricd represents a new Olricd instance.
type Olricd struct {
	logger *log.Logger
	config *olricdb.Config
	db     *olricdb.OlricDB
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
		c.Logging.Level = olricdb.DefaultLogLevel
	}

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "WARN", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel(strings.ToUpper(c.Logging.Level)),
		Writer:   logDest,
	}
	s.logger = log.New(logDest, "", log.LstdFlags)
	s.logger.SetOutput(filter)

	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var serializer olricdb.Serializer
	if c.Olricd.Serializer == "json" {
		serializer = olricdb.NewJSONSerializer()
	} else if c.Olricd.Serializer == "msgpack" {
		serializer = olricdb.NewMsgpackSerializer()
	} else if c.Olricd.Serializer == "gob" {
		serializer = olricdb.NewGobSerializer()
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
				fmt.Sprintf("failed to parse KeepAlivePeriod: '%s'", c.Olricd.KeepAlivePeriod))
		}
	}
	s.config = &olricdb.Config{
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
		Hasher:           olricdb.NewDefaultHasher(),
		Serializer:       serializer,
		KeepAlivePeriod:  keepAlivePeriod,
		MaxValueSize:     c.Olricd.MaxValueSize,
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
			s.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
			return err
		}
		return nil
	})
}

// Start starts a new olricd server instance and blocks until the server is closed.
func (s *Olricd) Start() error {
	// Wait for SIGTERM or SIGINT
	go s.waitForInterrupt()

	db, err := olricdb.New(s.config)
	if err != nil {
		return err
	}
	s.db = db
	s.logger.Printf("[INFO] olricd (pid: %d) has been started on %s", os.Getpid(), s.config.Name)
	s.errgr.Go(func() error {
		if err = s.db.Start(); err != nil {
			s.logger.Printf("[ERROR] Failed to run OlricDB: %v", err)
			return err
		}
		return nil
	})
	return s.errgr.Wait()
}
