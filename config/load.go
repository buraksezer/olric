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

package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/buraksezer/olric/config/internal/loader"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// Load reads and loads Olric configuration.
func Load(filename string) (*Config, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("file doesn't exists: %s", filename)
	}
	if err := unix.Access(filename, unix.R_OK); err != nil {
		return nil, fmt.Errorf("file doesn't readable: %s", filename)
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}
	c, err := loader.New(data)
	if err != nil {
		return nil, err
	}

	var logOutput io.Writer
	if c.Logging.Output == "stderr" {
		logOutput = os.Stderr
	} else if c.Logging.Output == "stdout" {
		logOutput = os.Stdout
	} else {
		logOutput = os.Stderr
	}
	if c.Logging.Level == "" {
		c.Logging.Level = DefaultLogLevel
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

	rawMc, err := NewMemberlistConfig(c.Memberlist.Environment)
	if err != nil {
		return nil, err
	}

	mc, err := processMemberlistConfig(c, rawMc)
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
	cacheConfig, err := processCacheConfig(c)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		BindAddr:            c.Olricd.BindAddr,
		BindPort:            c.Olricd.BindPort,
		Interface:           c.Olricd.Interface,
		ServiceDiscovery:    c.ServiceDiscovery,
		MemberlistInterface: c.Memberlist.Interface,
		MemberlistConfig:    mc,
		LogLevel:            c.Logging.Level,
		JoinRetryInterval:   joinRetryInterval,
		MaxJoinAttempts:     c.Memberlist.MaxJoinAttempts,
		Peers:               c.Memberlist.Peers,
		PartitionCount:      c.Olricd.PartitionCount,
		ReplicaCount:        c.Olricd.ReplicaCount,
		WriteQuorum:         c.Olricd.WriteQuorum,
		ReadQuorum:          c.Olricd.ReadQuorum,
		ReplicationMode:     c.Olricd.ReplicationMode,
		ReadRepair:          c.Olricd.ReadRepair,
		LoadFactor:          c.Olricd.LoadFactor,
		MemberCountQuorum:   c.Olricd.MemberCountQuorum,
		Logger:              log.New(logOutput, "", log.LstdFlags),
		LogOutput:           logOutput,
		LogVerbosity:        c.Logging.Verbosity,
		Hasher:              hasher.NewDefaultHasher(),
		Serializer:          sr,
		KeepAlivePeriod:     keepAlivePeriod,
		RequestTimeout:      requestTimeout,
		Cache:               cacheConfig,
		TableSize:           c.Olricd.TableSize,
	}
	if err := cfg.Sanitize(); err != nil {
		return nil, err
	}
	return cfg, nil
}
