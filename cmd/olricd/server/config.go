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

package server

import (
	"os"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultConfigFile is the default configuration file path on a Unix-based operating system.
	DefaultConfigFile = "olricd.toml"

	// EnvConfigFile is the name of environment variable which can be used to override default configuration file path.
	EnvConfigFile = "OLRICD_CONFIG"
)

type olricd struct {
	Name            string  `toml:"name"`
	CertFile        string  `toml:"certFile"`
	KeyFile         string  `toml:"keyFile"`
	BackupMode      int     `toml:"backupMode"`
	PartitionCount  uint64  `toml:"partitionCount"`
	BackupCount     int     `toml:"backupCount"`
	LoadFactor      float64 `toml:"loadFactor"`
	Serializer      string  `toml:"serializer"`
	KeepAlivePeriod string  `toml:"keepAlivePeriod"`
	MaxValueSize    int     `toml:"maxValueSize"`
}

type snapshot struct {
	Enabled        bool    `toml:"enabled"`
	Interval       string  `toml:"interval"`
	Dir            string  `toml:"dir"`
	GCInterval     string  `toml:"gcInterval"`
	GCDiscardRatio float64 `toml:"gcDiscardRatio"`
}

// logging contains configuration variables of logging section of config file.
type logging struct {
	Level  string `toml:"level"`
	Output string `toml:"output"`
}

type memberlist struct {
	Environment         string   `toml:"environment"`
	Addr                string   `toml:"addr"`
	EnableCompression   bool     `toml:"enableCompression"`
	Peers               []string `toml:"peers"`
	IndirectChecks      int      `toml:"indirectChecks"`
	RetransmitMult      int      `toml:"retransmitMult"`
	SuspicionMult       int      `toml:"suspicionMult"`
	TCPTimeout          string   `toml:"tcpTimeout"`
	PushPullInterval    string   `toml:"pushPullInterval"`
	ProbeTimeout        string   `toml:"probeTimeout"`
	ProbeInterval       string   `toml:"probeInterval"`
	GossipInterval      string   `toml:"gossipInterval"`
	GossipToTheDeadTime string   `toml:"gossipToTheDeadTime"`
}

// Config is the main configuration struct
type Config struct {
	Memberlist memberlist
	Logging    logging
	Olricd     olricd
	Snapshot   snapshot
}

// NewConfig creates a new configuration object of olricd
func NewConfig(path string) (*Config, error) {
	if len(path) == 0 {
		path = os.Getenv(EnvConfigFile)
	}
	if len(path) == 0 {
		path = DefaultConfigFile
	}
	var c Config
	if _, err := toml.DecodeFile(path, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
