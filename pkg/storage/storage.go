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

package storage // import "github.com/buraksezer/olric/pkg/storage"

import (
	"errors"
	"fmt"
	"plugin"
)

// ErrKeyTooLarge is an error that indicates the given key is large than the determined key size.
// The current maximum key length is 256.
var ErrKeyTooLarge = errors.New("key too large")

// ErrKeyNotFound is an error that indicates that the requested key could not be found in the DB.
var ErrKeyNotFound = errors.New("key not found")

// ErrNotImplemented means that the interface implementation does not support
// the functionality required to fulfill the request.
var ErrNotImplemented = errors.New("not implemented yet")

func LoadAsPlugin(pluginPath string) (Engine, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open plugin: %w", err)
	}
	tmp, err := plug.Lookup("StorageEngines")
	if err != nil {
		return nil, fmt.Errorf("failed to lookup StorageEngines symbol: %w", err)
	}
	impl, ok := tmp.(Engine)
	if !ok {
		return nil, fmt.Errorf("unable to assert type to StorageEngines")
	}
	return impl, nil
}
