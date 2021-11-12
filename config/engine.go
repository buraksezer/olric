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
	"os"

	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/pkg/storage"
)

// Engine contains storage engine configuration and their implementations.
// If you don't have a custom storage engine implementation or configuration for
// the default one, just call NewStorageEngine() function to use it with sane defaults.
type Engine struct {
	// Plugins is an array that contains the paths of storage engine plugins.
	// These plugins have to implement storage.Engine interface.
	Plugin string

	Name string

	Implementation storage.Engine

	// Config is a map that contains configuration of the storage engines, for
	// both plugins and imported ones. If you want to use a storage engine other
	// than the default one, you must set configuration for it.
	Config map[string]interface{}
}

// NewEngine initializes StorageEngine configuration with sane defaults.
// Olric will set its own storage engine implementation and related configuration,
// if there is no other engine.
func NewEngine() *Engine {
	return &Engine{
		Config: make(map[string]interface{}),
	}
}

// Validate finds errors in the current configuration.
func (s *Engine) Validate() error {
	_, err := os.Stat(s.Plugin)
	if os.IsNotExist(err) {
		return fmt.Errorf("storage engine plugin could not be found on disk: %s", s.Plugin)
	}
	return err
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (s *Engine) Sanitize() error {
	if s.Implementation == nil {
		s.Implementation = &kvstore.KVStore{}
		s.Name = DefaultStorageEngine
		s.Config = kvstore.DefaultConfig().ToMap()
	}
	return nil
}

// Interface guard
var _ IConfig = (*Engine)(nil)
