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
	"github.com/buraksezer/olric/stats"
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

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *EmbeddedDMap) Incr(key string, delta int) (int, error) {
	return dm.dm.Incr(key, delta)
}

func (dm *EmbeddedDMap) Delete(ctx context.Context, key string) error {
	return dm.dm.Delete(ctx, key)
}

func (dm *EmbeddedDMap) Get(ctx context.Context, key string) (*GetResponse, error) {
	result, err := dm.dm.Get(ctx, key)
	if err != nil {
		return nil, convertDMapError(err)
	}

	return &GetResponse{
		entry: result,
	}, nil
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

func CollectRuntime() StatsOption {
	return func(cfg *statsConfig) {
		cfg.CollectRuntime = true
	}
}

// Stats exposes some useful metrics to monitor an Olric node.
func (e *EmbeddedClient) Stats(options ...StatsOption) (stats.Stats, error) {
	if err := e.db.isOperable(); err != nil {
		// this node is not bootstrapped yet.
		return stats.Stats{}, err
	}
	var cfg statsConfig
	for _, opt := range options {
		opt(&cfg)
	}
	return e.db.stats(cfg), nil
}

func (e *EmbeddedClient) Close(_ context.Context) error {
	return nil
}

// Ping sends a dummy protocol message to the given host. This is useful to
// measure RTT between hosts. It also can be used as aliveness check.
func (e *EmbeddedClient) Ping(addr string) error {
	_, err := e.db.ping(addr, "")
	return err
}

// PingWithMessage sends a dummy protocol message to the given host. This is useful to
// measure RTT between hosts. It also can be used as aliveness check.
func (e *EmbeddedClient) PingWithMessage(addr, message string) (string, error) {
	response, err := e.db.ping(addr, message)
	if err != nil {
		return "", err
	}
	return string(response), nil
}

var (
	_ Client = (*EmbeddedClient)(nil)
	_ DMap   = (*EmbeddedDMap)(nil)
)
