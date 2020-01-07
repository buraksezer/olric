// Copyright 2018-2019 Burak Sezer
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
	"syscall"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Olricd represents a new Olricd instance.
type Olricd struct {
	log    *log.Logger
	config *config.Config
	db     *olric.Olric
	errgr  errgroup.Group
}

func prepareCacheConfig(c *Config) (*config.CacheConfig, error) {
	res := &config.CacheConfig{}
	if c.Cache.MaxIdleDuration != "" {
		maxIdleDuration, err := time.ParseDuration(c.Cache.MaxIdleDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse cache.MaxIdleDuration")
		}
		res.MaxIdleDuration = maxIdleDuration
	}
	if c.Cache.TTLDuration != "" {
		ttlDuration, err := time.ParseDuration(c.Cache.TTLDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse cache.TTLDuration")
		}
		res.TTLDuration = ttlDuration
	}
	res.NumEvictionWorkers = c.Cache.NumEvictionWorkers
	res.MaxKeys = c.Cache.MaxKeys
	res.MaxInuse = c.Cache.MaxInuse
	res.EvictionPolicy = config.EvictionPolicy(c.Cache.EvictionPolicy)
	res.LRUSamples = c.Cache.LRUSamples
	if c.DMaps != nil {
		res.DMapConfigs = make(map[string]config.DMapCacheConfig)
		for name, dc := range c.DMaps {
			cc := config.DMapCacheConfig{
				MaxInuse:       dc.MaxInuse,
				MaxKeys:        dc.MaxKeys,
				EvictionPolicy: config.EvictionPolicy(dc.EvictionPolicy),
				LRUSamples:     dc.LRUSamples,
			}
			if dc.MaxIdleDuration != "" {
				maxIdleDuration, err := time.ParseDuration(dc.MaxIdleDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse cache.%s.MaxIdleDuration", name)
				}
				cc.MaxIdleDuration = maxIdleDuration
			}
			if dc.TTLDuration != "" {
				ttlDuration, err := time.ParseDuration(dc.TTLDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse cache.%s.TTLDuration", name)
				}
				cc.TTLDuration = ttlDuration
			}
			res.DMapConfigs[name] = cc
		}
	}
	return res, nil
}

// New creates a new Server instance
func New(c *Config) (*Olricd, error) {
	s := &Olricd{}
	var logOutput io.Writer
	if c.Logging.Output == "stderr" {
		logOutput = os.Stderr
	} else if c.Logging.Output == "stdout" {
		logOutput = os.Stdout
	} else {
		logOutput = os.Stderr
	}
	if c.Logging.Level == "" {
		c.Logging.Level = config.DefaultLogLevel
	}

	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var sr serializer.Serializer
	if c.Olricd.Serializer == "json" {
		sr = serializer.NewJSONSerializer()
	} else if c.Olricd.Serializer == "msgpack" {
		sr = serializer.NewMsgpackSerializer()
	} else if c.Olricd.Serializer == "gob" {
		sr = serializer.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", c.Olricd.Serializer)
	}

	mc, err := newMemberlistConf(c)
	if err != nil {
		return nil, err
	}

	var joinRetryInterval, keepAlivePeriod, requestTimeout time.Duration
	if c.Olricd.KeepAlivePeriod != "" {
		keepAlivePeriod, err = time.ParseDuration(c.Olricd.KeepAlivePeriod)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.keepAlivePeriod: '%s'", c.Olricd.KeepAlivePeriod))
		}
	}
	if c.Olricd.RequestTimeout != "" {
		requestTimeout, err = time.ParseDuration(c.Olricd.RequestTimeout)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.requestTimeout: '%s'", c.Olricd.RequestTimeout))
		}
	}
	if c.Memberlist.JoinRetryInterval != "" {
		joinRetryInterval, err = time.ParseDuration(c.Memberlist.JoinRetryInterval)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse memberlist.joinRetryInterval: '%s'",
					c.Memberlist.JoinRetryInterval))
		}
	}
	cacheConfig, err := prepareCacheConfig(c)
	if err != nil {
		return nil, err
	}

	s.log = log.New(logOutput, "", log.LstdFlags)
	s.config = &config.Config{
		Name:              c.Olricd.Name,
		MemberlistConfig:  mc,
		LogLevel:          c.Logging.Level,
		JoinRetryInterval: joinRetryInterval,
		MaxJoinAttempts:   c.Memberlist.MaxJoinAttempts,
		Peers:             c.Memberlist.Peers,
		PartitionCount:    c.Olricd.PartitionCount,
		ReplicaCount:      c.Olricd.ReplicaCount,
		WriteQuorum:       c.Olricd.WriteQuorum,
		ReadQuorum:        c.Olricd.ReadQuorum,
		ReplicationMode:   c.Olricd.ReplicationMode,
		ReadRepair:        c.Olricd.ReadRepair,
		LoadFactor:        c.Olricd.LoadFactor,
		MemberCountQuorum: c.Olricd.MemberCountQuorum,
		Logger:            s.log,
		LogOutput:         logOutput,
		LogVerbosity:      c.Logging.Verbosity,
		Hasher:            hasher.NewDefaultHasher(),
		Serializer:        sr,
		KeepAlivePeriod:   keepAlivePeriod,
		RequestTimeout:    requestTimeout,
		Cache:             cacheConfig,
		TableSize:         c.Olricd.TableSize,
	}
	return s, nil
}

func (s *Olricd) waitForInterrupt() {
	shutDownChan := make(chan os.Signal, 1)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	ch := <-shutDownChan
	s.log.Printf("[olricd] Signal catched: %s", ch.String())

	// Awaits for shutdown
	s.errgr.Go(func() error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := s.db.Shutdown(ctx); err != nil {
			s.log.Printf("[olricd] Failed to shutdown Olric: %v", err)
			return err
		}
		return nil
	})

	// This is not a goroutine leak. The process will quit.
	go func() {
		s.log.Printf("[olricd] Awaiting for background tasks")
		s.log.Printf("[olricd] Press CTRL+C or send SIGTERM/SIGINT to quit immediately")
		forceQuitCh := make(chan os.Signal, 1)
		signal.Notify(forceQuitCh, syscall.SIGTERM, syscall.SIGINT)
		ch := <-forceQuitCh
		s.log.Printf("[olricd] Signal catched: %s", ch.String())
		s.log.Printf("[olricd] Quits with exit code 1")
		os.Exit(1)
	}()
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
	s.log.Printf("[olricd] pid: %d has been started on %s", os.Getpid(), s.config.Name)
	s.errgr.Go(func() error {
		if err = s.db.Start(); err != nil {
			s.log.Printf("[olricd] Failed to run Olric: %v", err)
			return err
		}
		return nil
	})
	return s.errgr.Wait()
}
