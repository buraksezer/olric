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

package server

import (
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

const (
	// DefaultConfigFile is the default configuration file path on a Unix-based operating system.
	DefaultConfigFile = "olricd.yaml"

	// EnvConfigFile is the name of environment variable which can be used to override default configuration file path.
	EnvConfigFile = "OLRICD_CONFIG"
)

type olricd struct {
	Name              string  `yaml:"name"`
	ReplicationMode   int     `yaml:"replicationMode"`
	PartitionCount    uint64  `yaml:"partitionCount"`
	LoadFactor        float64 `yaml:"loadFactor"`
	Serializer        string  `yaml:"serializer"`
	KeepAlivePeriod   string  `yaml:"keepAlivePeriod"`
	RequestTimeout    string  `yaml:"requestTimeout"`
	ReplicaCount      int     `yaml:"replicaCount"`
	WriteQuorum       int     `yaml:"writeQuorum"`
	ReadQuorum        int     `yaml:"readQuorum"`
	ReadRepair        bool    `yaml:"readRepair"`
	TableSize         int     `yaml:"tableSize"`
	MemberCountQuorum int32   `yaml:"memberCountQuorum"`
}

// logging contains configuration variables of logging section of config file.
type logging struct {
	Verbosity int32  `yaml:"verbosity"`
	Level     string `yaml:"level"`
	Output    string `yaml:"output"`
}

type memberlist struct {
	Environment             string   `yaml:"environment"` // required
	BindAddr                string   `yaml:"bindAddr"`    // required
	BindPort                int      `yaml:"bindPort"`    // required
	EnableCompression       *bool    `yaml:"enableCompression"`
	JoinRetryInterval       string   `yaml:"joinRetryInterval"` // required
	MaxJoinAttempts         int      `yaml:"maxJoinAttempts"`   // required
	Peers                   []string `yaml:"peers"`
	IndirectChecks          *int     `yaml:"indirectChecks"`
	RetransmitMult          *int     `yaml:"retransmitMult"`
	SuspicionMult           *int    `yaml:"suspicionMult"`
	TCPTimeout              *string `yaml:"tcpTimeout"`
	PushPullInterval        *string `yaml:"pushPullInterval"`
	ProbeTimeout            *string `yaml:"probeTimeout"`
	ProbeInterval           *string `yaml:"probeInterval"`
	GossipInterval          *string `yaml:"gossipInterval"`
	GossipToTheDeadTime     *string `yaml:"gossipToTheDeadTime"`
	AdvertiseAddr           *string `yaml:"advertiseAddr"`
	AdvertisePort           *int    `yaml:"advertisePort"`
	SuspicionMaxTimeoutMult *int    `yaml:"suspicionMaxTimeoutMult"`
	DisableTCPPings         *bool   `yaml:"disableTCPPings"`
	AwarenessMaxMultiplier  *int    `yaml:"awarenessMaxMultiplier"`
	GossipNodes             *int    `yaml:"gossipNodes"`
	GossipVerifyIncoming    *bool   `yaml:"gossipVerifyIncoming"`
	GossipVerifyOutgoing    *bool   `yaml:"gossipVerifyOutgoing"`
	DNSConfigPath           *string `yaml:"dnsConfigPath"`
	HandoffQueueDepth       *int    `yaml:"handoffQueueDepth"`
	UDPBufferSize           *int    `yaml:"udpBufferSize"`
}

type cache struct {
	NumEvictionWorkers int64  `yaml:"numEvictionWorkers"`
	MaxIdleDuration    string `yaml:"maxIdleDuration"`
	TTLDuration        string `yaml:"ttlDuration"`
	MaxKeys            int    `yaml:"maxKeys"`
	MaxInuse           int    `yaml:"maxInuse"`
	LRUSamples         int    `yaml:"lruSamples"`
	EvictionPolicy     string `yaml:"evictionPolicy"`
}

// Config is the main configuration struct
type Config struct {
	Memberlist memberlist
	Logging    logging
	Olricd     olricd
	Cache      cache
	DMaps      map[string]cache
}

// NewConfig creates a new configuration instance of olricd
func NewConfig(path string) (*Config, error) {
	envPath := os.Getenv(EnvConfigFile)
	if envPath != "" {
		path = envPath
	}
	if path == "" {
		path = DefaultConfigFile
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
