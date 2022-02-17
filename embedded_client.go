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

/*
// Iterator implements distributed query on DMaps.
type Iterator struct {
	mtx sync.Mutex

	pos      int
	page     []string
	dm       DMap
	allKeys  map[string]struct{}
	finished map[uint64]struct{}
	cursors  map[uint64]uint64 // member id => cursor
	partID   uint64            // current partition id
	config   *dmap.ScanConfig
	ctx      context.Context
	cancel   context.CancelFunc
}

func (i *Iterator) updateIterator(keys []string, cursor, ownerID uint64) {
	if cursor == 0 {
		i.finished[ownerID] = struct{}{}
	}
	i.cursors[ownerID] = cursor
	for _, key := range keys {
		if _, ok := i.allKeys[key]; !ok {
			i.page = append(i.page, key)
			i.allKeys[key] = struct{}{}
		}
	}
}

func (i *Iterator) scanOnOwners(owners []discovery.Member) error {
	for _, owner := range owners {
		if _, ok := i.finished[owner.ID]; ok {
			continue
		}
		if owner.CompareByID(i.dm.s.rt.This()) {
			keys, cursor, err := i.dm.scan(i.partID, i.cursors[owner.ID], i.config)
			if err != nil {
				return err
			}
			i.updateIterator(keys, cursor, owner.ID)
			continue
		}

		s := protocol.NewScan(i.partID, i.dm.Name(), i.cursors[owner.ID])
		if i.config.HasCount {
			s.SetCount(i.config.Count)
		}
		if i.config.HasMatch {
			s.SetMatch(s.Match)
		}
		scanCmd := s.Command(i.ctx)
		rc := i.dm.s.client.Get(owner.String())
		err := rc.Process(i.ctx, scanCmd)
		if err != nil {
			return err
		}
		keys, cursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		i.updateIterator(keys, cursor, owner.ID)
	}

	return nil
}

func (i *Iterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *Iterator) reset() {
	// Reset
	for memberID := range i.cursors {
		delete(i.cursors, memberID)
		delete(i.finished, memberID)
	}
	i.resetPage()
}

func (i *Iterator) next() bool {
	if len(i.page) != 0 {
		i.pos++
		if i.pos <= len(i.page) {
			return true
		}
	}

	i.resetPage()

	primaryOwners := i.dm.s.primary.PartitionOwnersByID(i.partID)
	i.config.Replica = false
	if err := i.scanOnOwners(primaryOwners); err != nil {
		return false
	}

	replicaOwners := i.dm.s.backup.PartitionOwnersByID(i.partID)
	i.config.Replica = true
	if err := i.scanOnOwners(replicaOwners); err != nil {
		return false
	}

	if len(i.page) == 0 {
		i.partID++
		if i.dm.s.config.PartitionCount <= i.partID {
			return false
		}
		i.reset()
		return i.next()
	}
	i.pos = 1
	return true
}

func (i *Iterator) Next() bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-i.ctx.Done():
		return false
	default:
	}

	return i.next()
}

func (i *Iterator) Key() string {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var key string
	if i.pos > 0 && i.pos <= len(i.page) {
		key = i.page[i.pos-1]
	}
	return key
}

func (i *Iterator) Close() {
	select {
	case <-i.ctx.Done():
		return
	default:
	}
	i.cancel()
}*/
