// Copyright 2018-2021 Burak Sezer
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
	"time"

	"github.com/buraksezer/olric/config/internal/loader"
	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/memberlist"
	"github.com/pkg/errors"
)

// mapYamlToConfig maps a parsed YAML to related configuration struct.
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
				if dst.Field(i).Type() == reflect.TypeOf(time.Duration(0)) {
					rawValue := src.Field(j).String()
					if rawValue != "" {
						value, err := time.ParseDuration(rawValue)
						if err != nil {
							return err
						}
						dst.Field(i).Set(reflect.ValueOf(value))
					}
					continue
				}
				return fmt.Errorf("failed to map %s to an appropriate field in config", dst.Type().Field(j).Name)
			}
		}
	}
	return nil
}

func loadDMapConfig(c *loader.Loader) (*DMaps, error) {
	res := &DMaps{}
	if c.DMaps.MaxIdleDuration != "" {
		maxIdleDuration, err := time.ParseDuration(c.DMaps.MaxIdleDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse dmap.MaxIdleDuration")
		}
		res.MaxIdleDuration = maxIdleDuration
	}
	if c.DMaps.TTLDuration != "" {
		ttlDuration, err := time.ParseDuration(c.DMaps.TTLDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse dmap.TTLDuration")
		}
		res.TTLDuration = ttlDuration
	}
	if c.DMaps.CheckEmptyFragmentsInterval != "" {
		checkEmptyFragmentsInterval, err := time.ParseDuration(c.DMaps.CheckEmptyFragmentsInterval)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse dmap.MaxIdleDuration")
		}
		res.CheckEmptyFragmentsInterval = checkEmptyFragmentsInterval
	}
	res.NumEvictionWorkers = c.DMaps.NumEvictionWorkers
	res.MaxKeys = c.DMaps.MaxKeys
	res.MaxInuse = c.DMaps.MaxInuse
	res.EvictionPolicy = EvictionPolicy(c.DMaps.EvictionPolicy)
	res.LRUSamples = c.DMaps.LRUSamples
	res.StorageEngine = c.DMaps.StorageEngine
	if c.DMaps.Custom != nil {
		res.Custom = make(map[string]DMap)
		for name, dc := range c.DMaps.Custom {
			cc := DMap{
				MaxInuse:       dc.MaxInuse,
				MaxKeys:        dc.MaxKeys,
				EvictionPolicy: EvictionPolicy(dc.EvictionPolicy),
				LRUSamples:     dc.LRUSamples,
				StorageEngine:  dc.StorageEngine,
			}
			if dc.MaxIdleDuration != "" {
				maxIdleDuration, err := time.ParseDuration(dc.MaxIdleDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse dmaps.%s.MaxIdleDuration", name)
				}
				cc.MaxIdleDuration = maxIdleDuration
			}
			if dc.TTLDuration != "" {
				ttlDuration, err := time.ParseDuration(dc.TTLDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse dmaps.%s.TTLDuration", name)
				}
				cc.TTLDuration = ttlDuration
			}
			res.Custom[name] = cc
		}
	}
	return res, nil
}

// loadMemberlistConfig creates a new *memberlist.Config by parsing olricd.yaml
func loadMemberlistConfig(c *loader.Loader, mc *memberlist.Config) (*memberlist.Config, error) {
	var err error
	if c.Memberlist.BindAddr == "" {
		name, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		c.Memberlist.BindAddr = name
	}
	mc.BindAddr = c.Memberlist.BindAddr
	mc.BindPort = c.Memberlist.BindPort

	if c.Memberlist.EnableCompression != nil {
		mc.EnableCompression = *c.Memberlist.EnableCompression
	}

	if c.Memberlist.TCPTimeout != nil {
		mc.TCPTimeout, err = time.ParseDuration(*c.Memberlist.TCPTimeout)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.IndirectChecks != nil {
		mc.IndirectChecks = *c.Memberlist.IndirectChecks
	}

	if c.Memberlist.RetransmitMult != nil {
		mc.RetransmitMult = *c.Memberlist.RetransmitMult
	}

	if c.Memberlist.SuspicionMult != nil {
		mc.SuspicionMult = *c.Memberlist.SuspicionMult
	}

	if c.Memberlist.PushPullInterval != nil {
		mc.PushPullInterval, err = time.ParseDuration(*c.Memberlist.PushPullInterval)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.ProbeTimeout != nil {
		mc.ProbeTimeout, err = time.ParseDuration(*c.Memberlist.ProbeTimeout)
		if err != nil {
			return nil, err
		}
	}
	if c.Memberlist.ProbeInterval != nil {
		mc.ProbeInterval, err = time.ParseDuration(*c.Memberlist.ProbeInterval)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.GossipInterval != nil {
		mc.GossipInterval, err = time.ParseDuration(*c.Memberlist.GossipInterval)
		if err != nil {
			return nil, err
		}
	}
	if c.Memberlist.GossipToTheDeadTime != nil {
		mc.GossipToTheDeadTime, err = time.ParseDuration(*c.Memberlist.GossipToTheDeadTime)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.AdvertiseAddr != nil {
		mc.AdvertiseAddr = *c.Memberlist.AdvertiseAddr
	}

	if c.Memberlist.AdvertisePort != nil {
		mc.AdvertisePort = *c.Memberlist.AdvertisePort
	} else {
		mc.AdvertisePort = mc.BindPort
	}

	if c.Memberlist.SuspicionMaxTimeoutMult != nil {
		mc.SuspicionMaxTimeoutMult = *c.Memberlist.SuspicionMaxTimeoutMult
	}

	if c.Memberlist.DisableTCPPings != nil {
		mc.DisableTcpPings = *c.Memberlist.DisableTCPPings
	}

	if c.Memberlist.AwarenessMaxMultiplier != nil {
		mc.AwarenessMaxMultiplier = *c.Memberlist.AwarenessMaxMultiplier
	}

	if c.Memberlist.GossipNodes != nil {
		mc.GossipNodes = *c.Memberlist.GossipNodes
	}
	if c.Memberlist.GossipVerifyIncoming != nil {
		mc.GossipVerifyIncoming = *c.Memberlist.GossipVerifyIncoming
	}
	if c.Memberlist.GossipVerifyOutgoing != nil {
		mc.GossipVerifyOutgoing = *c.Memberlist.GossipVerifyOutgoing
	}

	if c.Memberlist.DNSConfigPath != nil {
		mc.DNSConfigPath = *c.Memberlist.DNSConfigPath
	}

	if c.Memberlist.HandoffQueueDepth != nil {
		mc.HandoffQueueDepth = *c.Memberlist.HandoffQueueDepth
	}
	if c.Memberlist.UDPBufferSize != nil {
		mc.UDPBufferSize = *c.Memberlist.UDPBufferSize
	}
	return mc, nil
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
	switch {
	case c.Logging.Output == "stderr":
		logOutput = os.Stderr
	case c.Logging.Output == "stdout":
		logOutput = os.Stdout
	default:
		logOutput = os.Stderr
	}

	if c.Logging.Level == "" {
		c.Logging.Level = DefaultLogLevel
	}

	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var sr serializer.Serializer
	switch {
	case c.Olricd.Serializer == "json":
		sr = serializer.NewJSONSerializer()
	case c.Olricd.Serializer == "msgpack":
		sr = serializer.NewMsgpackSerializer()
	case c.Olricd.Serializer == "gob":
		sr = serializer.NewGobSerializer()
	default:
		return nil, fmt.Errorf("invalid serializer: %s", c.Olricd.Serializer)
	}

	rawMc, err := NewMemberlistConfig(c.Memberlist.Environment)
	if err != nil {
		return nil, err
	}

	memberlistConfig, err := loadMemberlistConfig(c, rawMc)
	if err != nil {
		return nil, err
	}

	var (
		joinRetryInterval,
		keepAlivePeriod,
		bootstrapTimeout,
		routingTablePushInterval,
		leaveTimeout time.Duration
	)

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
	if c.Olricd.RoutingTablePushInterval != "" {
		routingTablePushInterval, err = time.ParseDuration(c.Olricd.RoutingTablePushInterval)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.routingTablePushInterval: '%s'", c.Olricd.RoutingTablePushInterval))
		}
	}
	if c.Olricd.LeaveTimeout != "" {
		leaveTimeout, err = time.ParseDuration(c.Olricd.LeaveTimeout)
		if err != nil {
			return nil, errors.WithMessage(err,
				fmt.Sprintf("failed to parse olricd.leaveTimeout: '%s'", c.Olricd.LeaveTimeout))
		}
	}

	clientConfig := Client{}
	err = mapYamlToConfig(&clientConfig, &c.Client)
	if err != nil {
		return nil, err
	}

	dmapConfig, err := loadDMapConfig(c)
	if err != nil {
		return nil, err
	}

	storageEngines := NewStorageEngine()
	storageEngines.Plugins = c.StorageEngines.Plugins
	storageEngines.Config = c.StorageEngines.Config

	cfg := &Config{
		BindAddr:                 c.Olricd.BindAddr,
		BindPort:                 c.Olricd.BindPort,
		Interface:                c.Olricd.Interface,
		ServiceDiscovery:         c.ServiceDiscovery,
		MemberlistInterface:      c.Memberlist.Interface,
		MemberlistConfig:         memberlistConfig,
		Client:                   &clientConfig,
		LogLevel:                 c.Logging.Level,
		JoinRetryInterval:        joinRetryInterval,
		RoutingTablePushInterval: routingTablePushInterval,
		MaxJoinAttempts:          c.Memberlist.MaxJoinAttempts,
		Peers:                    c.Memberlist.Peers,
		PartitionCount:           c.Olricd.PartitionCount,
		ReplicaCount:             c.Olricd.ReplicaCount,
		WriteQuorum:              c.Olricd.WriteQuorum,
		ReadQuorum:               c.Olricd.ReadQuorum,
		ReplicationMode:          c.Olricd.ReplicationMode,
		ReadRepair:               c.Olricd.ReadRepair,
		LoadFactor:               c.Olricd.LoadFactor,
		MemberCountQuorum:        c.Olricd.MemberCountQuorum,
		Logger:                   log.New(logOutput, "", log.LstdFlags),
		LogOutput:                logOutput,
		LogVerbosity:             c.Logging.Verbosity,
		Hasher:                   hasher.NewDefaultHasher(),
		Serializer:               sr,
		KeepAlivePeriod:          keepAlivePeriod,
		BootstrapTimeout:         bootstrapTimeout,
		LeaveTimeout:             leaveTimeout,
		DMaps:                    dmapConfig,
		StorageEngines:           storageEngines,
	}

	if err := cfg.Sanitize(); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
