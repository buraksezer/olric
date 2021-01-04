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

package dmap

import (
	"fmt"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
	"time"
)

func (dm *DMap) localExpire(e *env) error {
	ttl := timeoutToTTL(e.timeout)
	entry := e.fragment.storage.NewEntry()
	entry.SetTimestamp(e.timestamp)
	entry.SetTTL(ttl)
	err := e.fragment.storage.UpdateTTL(e.hkey, entry)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			err = ErrKeyNotFound
		}
		return err
	}
	dm.updateAccessLog(e.hkey)
	return nil
}

func (dm *DMap) asyncExpireOnCluster(e *env) error {
	req := e.toReq(protocol.OpExpireReplica)
	// Fire and forget mode.
	owners := dm.service.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		dm.service.wg.Add(1)
		go func(host discovery.Member) {
			defer dm.service.wg.Done()
			_, err := dm.service.client.RequestTo2(host.String(), req)
			if err != nil {
				if dm.service.log.V(3).Ok() {
					dm.service.log.V(3).Printf("[ERROR] Failed to set expire in async mode: %v", err)
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
	owners := dm.service.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		_, err := dm.service.client.RequestTo2(owner.String(), req)
		if err != nil {
			if dm.service.log.V(3).Ok() {
				dm.service.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
					owner, e.dmap, err)
			}
			continue
		}
		successful++
	}
	err := dm.localExpire(e)
	if err != nil {
		if dm.service.log.V(3).Ok() {
			dm.service.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				dm.service.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= dm.service.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (dm *DMap) callExpireOnCluster(e *env) error {
	f, err := dm.getFragment(e.dmap, e.hkey, partitions.PRIMARY)
	if err == errFragmentNotFound {
		return ErrKeyNotFound
	}
	if err != nil {
		return err
	}
	e.fragment = f
	f.Lock()
	defer f.Unlock()

	if dm.service.config.ReplicaCount == config.MinimumReplicaCount {
		// MinimumReplicaCount is 1. So it's enough to put the key locally. There is no
		// other replica host.
		return dm.localExpire(e)
	}

	if dm.service.config.ReplicationMode == config.AsyncReplicationMode {
		return dm.asyncExpireOnCluster(e)
	} else if dm.service.config.ReplicationMode == config.SyncReplicationMode {
		return dm.syncExpireOnCluster(e)
	}

	return fmt.Errorf("invalid replication mode: %v", dm.service.config.ReplicationMode)
}

func (dm *DMap) expire(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.service.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.service.rt.This()) {
		// We are on the partition owner.
		return dm.callExpireOnCluster(e)
	}
	// Redirect to the partition owner
	req := e.toReq(protocol.OpExpire)
	_, err := dm.service.client.RequestTo2(member.String(), req)
	return err
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (dm *DMap) Expire(key string, timeout time.Duration) error {
	w := &env{
		dmap:      dm.name,
		key:       key,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
	}
	return dm.expire(w)
}
