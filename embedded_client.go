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

package olric

import (
	"context"
	"github.com/buraksezer/olric/internal/dmap"
)

// EmbeddedClient is an Olric client implementation for embedded-member scenario.
type EmbeddedClient struct {
	db *Olric
}

// EmbeddedDMap is an DMap client implementation for embedded-member scenario.
type EmbeddedDMap struct {
	dm            *dmap.DMap
	name          string
	storageEngine string
}

func (dm *EmbeddedDMap) Put(ctx context.Context, key string, value interface{}, options ...PutOption) error {
	var pc dmap.PutConfig
	for _, opt := range options {
		opt(&pc)
	}
	return dm.dm.Put(ctx, key, value, &pc)
}

func (db *Olric) NewEmbeddedClient() *EmbeddedClient {
	return &EmbeddedClient{db: db}
}

func (e *EmbeddedClient) NewDMap(name string, options ...DMapOption) (DMap, error) {
	dm, err := e.db.dmap.NewDMap(name)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &EmbeddedDMap{
		dm:   dm,
		name: name,
	}, nil
}

var (
	//_ Client = (*EmbeddedClient)(nil)
	_ DMap = (*EmbeddedDMap)(nil)
)
