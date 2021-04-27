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
	"github.com/buraksezer/olric/pkg/storage"
)

// StorageEngines contains storage engine configuration and their implementations.
// If you don't have a custom storage engine implementation or configuration for
// the default one, just call NewStorageEngine() function to use it with sane defaults.
type StorageEngines struct {
	// Plugins is an array that contains the paths of storage engine plugins.
	// These plugins have to implement storage.Engine interface.
	Plugins []string

	// Impls is a map that contains storage engines with their names.
	// This is useful to import and use external storage engine implementations.
	Impls map[string]storage.Engine

	// Config is a map that contains configuration of the storage engines, for
	// both plugins and imported ones.
	Config map[string]map[string]interface{}
}

// NewStorageEngine initializes StorageEngine configuration with sane defaults.
// Olric will set its own storage engine implementation and related configuration,
// if there is no other engine.
func NewStorageEngine() *StorageEngines {
	return &StorageEngines{
		Plugins: []string{},
		Impls:   make(map[string]storage.Engine),
		Config:  make(map[string]map[string]interface{}),
	}
}
