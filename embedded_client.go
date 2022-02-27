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
	"time"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/stats"
)

// EmbeddedLockContext is returned by Lock and LockWithTimeout methods.
// It should be stored in a proper way to release the lock.
type EmbeddedLockContext struct {
	key   string
	token []byte
	dm    *EmbeddedDMap
}

// Unlock releases the lock.
func (l *EmbeddedLockContext) Unlock(ctx context.Context) error {
	err := l.dm.dm.Unlock(ctx, l.key, l.token)
	return convertDMapError(err)
}

// Lease takes the duration to update the expiry for the given Lock.
func (l *EmbeddedLockContext) Lease(ctx context.Context, duration time.Duration) error {
	err := l.dm.dm.Lease(ctx, l.key, l.token, duration)
	return convertDMapError(err)
}

// EmbeddedClient is an Olric rc implementation for embedded-member scenario.
type EmbeddedClient struct {
	db *Olric
}

// EmbeddedDMap is an DMap rc implementation for embedded-member scenario.
type EmbeddedDMap struct {
	config        *dmapConfig
	member        discovery.Member
	dm            *dmap.DMap
	client        *EmbeddedClient
	name          string
	storageEngine string
}

func (dm *EmbeddedDMap) Scan(ctx context.Context, options ...ScanOption) (Iterator, error) {
	var sc dmap.ScanConfig
	for _, opt := range options {
		opt(&sc)
	}
	if sc.Count == 0 {
		sc.Count = DefaultScanCount
	}
	ictx, cancel := context.WithCancel(ctx)
	return &EmbeddedIterator{
		client:   dm.client,
		dm:       dm.dm,
		config:   &sc,
		allKeys:  make(map[string]struct{}),
		finished: make(map[uint64]struct{}),
		cursors:  make(map[uint64]uint64),
		ctx:      ictx,
		cancel:   cancel,
	}, nil
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *EmbeddedDMap) Lock(ctx context.Context, key string, deadline time.Duration) (LockContext, error) {
	token, err := dm.dm.Lock(ctx, key, 0*time.Second, deadline)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &EmbeddedLockContext{
		key:   key,
		token: token,
		dm:    dm,
	}, nil
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *EmbeddedDMap) LockWithTimeout(ctx context.Context, key string, timeout, deadline time.Duration) (LockContext, error) {
	token, err := dm.dm.Lock(ctx, key, timeout, deadline)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &EmbeddedLockContext{
		key:   key,
		token: token,
		dm:    dm,
	}, nil
}

func (dm *EmbeddedDMap) Destroy(ctx context.Context) error {
	return dm.dm.Destroy(ctx)
}

func (dm *EmbeddedDMap) Expire(ctx context.Context, key string, timeout time.Duration) error {
	return dm.dm.Expire(ctx, key, timeout)
}

func (dm *EmbeddedDMap) Name() string {
	return dm.name
}

func (dm *EmbeddedDMap) GetPut(ctx context.Context, key string, value interface{}) (*GetResponse, error) {
	e, err := dm.dm.GetPut(ctx, key, value)
	if err != nil {
		return nil, err
	}
	return &GetResponse{
		entry: e,
	}, nil
}

func (dm *EmbeddedDMap) Decr(ctx context.Context, key string, delta int) (int, error) {
	return dm.dm.Decr(ctx, key, delta)
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *EmbeddedDMap) Incr(ctx context.Context, key string, delta int) (int, error) {
	return dm.dm.Incr(ctx, key, delta)
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
	err := dm.dm.Put(ctx, key, value, &pc)
	if err != nil {
		return convertDMapError(err)
	}
	return nil
}

func (e *EmbeddedClient) NewDMap(name string, options ...DMapOption) (DMap, error) {
	dm, err := e.db.dmap.NewDMap(name)
	if err != nil {
		return nil, convertDMapError(err)
	}

	var dc dmapConfig
	for _, opt := range options {
		opt(&dc)
	}

	return &EmbeddedDMap{
		config: &dc,
		dm:     dm,
		name:   name,
		client: e,
		member: e.db.rt.This(),
	}, nil
}

func CollectRuntime() StatsOption {
	return func(cfg *statsConfig) {
		cfg.CollectRuntime = true
	}
}

// Stats exposes some useful metrics to monitor an Olric node.
func (e *EmbeddedClient) Stats(_ context.Context, options ...StatsOption) (stats.Stats, error) {
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
func (e *EmbeddedClient) Ping(ctx context.Context, addr string) error {
	_, err := e.db.ping(ctx, addr, "")
	return err
}

// PingWithMessage sends a dummy protocol message to the given host. This is useful to
// measure RTT between hosts. It also can be used as aliveness check.
func (e *EmbeddedClient) PingWithMessage(ctx context.Context, addr, message string) (string, error) {
	response, err := e.db.ping(ctx, addr, message)
	if err != nil {
		return "", err
	}
	return string(response), nil
}

func (e *EmbeddedClient) RoutingTable(ctx context.Context) (RoutingTable, error) {
	return e.db.routingTable(ctx)
}

func (db *Olric) NewEmbeddedClient() *EmbeddedClient {
	return &EmbeddedClient{db: db}
}

var (
	_ Client = (*EmbeddedClient)(nil)
	_ DMap   = (*EmbeddedDMap)(nil)
)
