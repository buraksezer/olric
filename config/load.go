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
	"reflect"
	"strings"
	"time"

	"github.com/buraksezer/olric/config/internal/loader"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/pkg/errors"
)

func durationCondition(name string) bool {
	return strings.HasSuffix(name, "Duration") ||
		strings.HasSuffix(name, "Timeout") ||
		strings.HasSuffix(name, "Period")
}

func keepaliveCondition(name string, field reflect.Value) bool {
	return strings.ToUpper(name) == "KEEPALIVE" && field.Kind() == reflect.Int64
}

// mapYamlToConfig maps a parsed yaml to related configuration struct.
// TODO: Use this to create Olric and memberlist config from yaml file.
func mapYamlToConfig(rawDst, rawSrc interface{}) error {
	dst := reflect.ValueOf(rawDst).Elem()
	src := reflect.ValueOf(rawSrc).Elem()
	for j := 0; j < src.NumField(); j++ {
		for i := 0; i < dst.NumField(); i++ {
			if src.Type().Field(j).Name == dst.Type().Field(i).Name {
				if src.Field(j).Kind() == dst.Field(i).Kind() {
					dst.Field(i).Set(src.Field(j))
					continue
				}
				// Special cases
				name := src.Type().Field(j).Name
				if src.Field(j).Kind() == reflect.String && !src.Field(j).IsZero() {
					if durationCondition(name) || keepaliveCondition(name, dst.Field(i)) {
						value, err := time.ParseDuration(src.Field(j).String())
						if err != nil {
							return err
						}
						dst.Field(i).Set(reflect.ValueOf(value))
					}
				}
			}
		}
	}
	return nil
}

// Load reads and loads Olric configuration.
func Load(filename string) (*Config, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("file doesn't exists: %s", filename)
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

	var joinRetryInterval, keepAlivePeriod, bootstrapTimeout time.Duration
	if c.Olricd.KeepAlivePeriod != "" {
		keepAlivePeriod, err = time.ParseDuration(c.Olricd.KeepAlivePeriod)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.keepAlivePeriod: '%s'", c.Olricd.KeepAlivePeriod))
		}
	}
	if c.Olricd.BootstrapTimeout != "" {
		bootstrapTimeout, err = time.ParseDuration(c.Olricd.BootstrapTimeout)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.bootstrapTimeout: '%s'", c.Olricd.BootstrapTimeout))
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

	cc := Client{}
	err = mapYamlToConfig(&cc, &c.Client)
	if err != nil {
		return nil, err
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
		Client:              &cc,
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
		BootstrapTimeout:    bootstrapTimeout,
		Cache:               cacheConfig,
		TableSize:           c.Olricd.TableSize,
	}
	if err := cfg.Sanitize(); err != nil {
		return nil, err
	}
	return cfg, nil
}
