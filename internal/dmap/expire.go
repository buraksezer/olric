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

package dmap

import (
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
)

func (dm *DMap) localExpireOnReplica(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.BACKUP)
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	return dm.localExpire(e)
}

func (dm *DMap) localExpire(e *env) error {
	ttl := timeoutToTTL(e.timeout)
	entry := e.fragment.storage.NewEntry()
	entry.SetTimestamp(e.timestamp)
	entry.SetTTL(ttl)
	err := e.fragment.storage.UpdateTTL(e.hkey, entry)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
		}
		return err
	}
	return nil
}

func (dm *DMap) asyncExpireOnCluster(e *env) error {
	req := e.toReq(protocol.OpExpireReplica)
	// Fire and forget mode.
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		dm.s.wg.Add(1)
		go func(host discovery.Member) {
			defer dm.s.wg.Done()
			_, err := dm.s.requestTo(host.String(), req)
			if err != nil {
				if dm.s.log.V(3).Ok() {
					dm.s.log.V(3).Printf("[ERROR] Failed to set expire in async mode: %v", err)
				}
			}
		}(owner)
	}
	return dm.putOnFragment(e)
}

func (dm *DMap) syncExpireOnCluster(e *env) error {
	req := e.toReq(protocol.OpExpireReplica)

	// Quorum based replication.
	var successful int
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		_, err := dm.s.requestTo(owner.String(), req)
		if err != nil {
			if dm.s.log.V(3).Ok() {
				dm.s.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
					owner, e.dmap, err)
			}
			continue
		}
		successful++
	}
	err := dm.localExpire(e)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				dm.s.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= dm.s.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (dm *DMap) callExpireOnCluster(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}
	e.fragment = f
	f.Lock()
	defer f.Unlock()

	if dm.s.config.ReplicaCount == config.MinimumReplicaCount {
		// MinimumReplicaCount is 1. So it's enough to put the key locally. There is no
		// other replica host.
		return dm.localExpire(e)
	}

	switch dm.s.config.ReplicationMode {
	case config.AsyncReplicationMode:
		return dm.asyncExpireOnCluster(e)
	case config.SyncReplicationMode:
		return dm.syncExpireOnCluster(e)
	default:
		return fmt.Errorf("invalid replication mode: %v", dm.s.config.ReplicationMode)
	}
}

func (dm *DMap) expire(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// We are on the partition owner.
		return dm.callExpireOnCluster(e)
	}
	// Redirect to the partition owner
	req := e.toReq(protocol.OpExpire)
	_, err := dm.s.requestTo(member.String(), req)
	return err
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contain the key. It's thread-safe.
func (dm *DMap) Expire(key string, timeout time.Duration) error {
	e := &env{
		dmap:      dm.name,
		key:       key,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
	}
	return dm.expire(e)
}
