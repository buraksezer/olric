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
	"errors"
	"fmt"
	"plugin"

	"github.com/buraksezer/olric/internal/storage"
)

type StorageEngines struct {
	Plugins []string
	Engines map[string]storage.Engine
	Config  map[string]map[string]interface{}
}

func NewStorageEngine() *StorageEngines {
	return &StorageEngines{
		Plugins: []string{},
		Engines: make(map[string]storage.Engine),
		Config:  make(map[string]map[string]interface{}),
	}
}

func (s *StorageEngines) Register(engine interface{}) error {
	var impl storage.Engine
	if pluginPath, ok := engine.(string); ok {
		plug, err := plugin.Open(pluginPath)
		if err != nil {
			return fmt.Errorf("failed to open plugin: %w", err)
		}
		tmp, err := plug.Lookup("StorageEngines")
		if err != nil {
			return fmt.Errorf("failed to lookup StorageEngines symbol: %w", err)
		}
		impl, ok = tmp.(storage.Engine)
		if !ok {
			return fmt.Errorf("unable to assert type to StorageEngines")
		}
	} else if _, ok := engine.(storage.Engine); ok {
		impl, _ = engine.(storage.Engine)
	} else {
		return errors.New("invalid type for StorageEngines")
	}

	if impl.Name() == DefaultStorageEngine {
		return errors.New("cannot register the default storage engine")
	}

	if _, ok := s.Engines[impl.Name()]; ok {
		return fmt.Errorf("already registered: %s", impl.Name())
	}
	s.Engines[impl.Name()] = impl
	return nil
}
