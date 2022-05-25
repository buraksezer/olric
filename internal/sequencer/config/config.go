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

package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v2"
)

// Logging contains configuration variables of logging section of config file.
type Logging struct {
	Verbosity int32  `json:"verbosity" yaml:"verbosity"`
	Level     string `json:"level" yaml:"level"`
	Output    string `json:"output" yaml:"output"`
}

type OlricSequencer struct {
	BindAddr        string `json:"bindAddr" yaml:"bindAddr"`
	BindPort        int    `json:"bindPort" yaml:"bindPort"`
	KeepAlivePeriod string `json:"keepAlivePeriod" yaml:"keepAlivePeriod"`
	DataDir         string `json:"dataDir" yaml:"dataDir"`
}

type Config struct {
	OlricSequencer OlricSequencer `json:"olric-sequencer" yaml:"olric-sequencer"`
	Logging        Logging        `json:"logging" yaml:"logging"`
}

func New(filename string) (*Config, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("file doesn't exists: %s", filename)
	}
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
