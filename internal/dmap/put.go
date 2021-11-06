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
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/pkg/storage"
)

const (
	IfNotFound = int16(1) << iota
	IfFound
)

// EntriesTotal is the total number of entries(including replicas)
// stored during the life of this instance.
var EntriesTotal = stats.NewInt64Counter()

var (
	ErrKeyFound    = neterrors.New(protocol.StatusErrKeyFound, "key found")
	ErrWriteQuorum = neterrors.New(protocol.StatusErrWriteQuorum, "write quorum cannot be reached")
	ErrKeyTooLarge = neterrors.New(protocol.StatusErrKeyTooLarge, "key too large")
)

// putOnFragment calls underlying storage engine's Put method to store the key/value pair. It's not thread-safe.
func (dm *DMap) putOnFragment(e *env) error {
	entry := e.fragment.storage.NewEntry()
	entry.SetKey(e.key)
	entry.SetValue(e.value)
	entry.SetTTL(timeoutToTTL(e.timeout))
	entry.SetTimestamp(e.timestamp)

	err := e.fragment.storage.Put(e.hkey, entry)
	if errors.Is(err, storage.ErrKeyTooLarge) {
		err = ErrKeyTooLarge
	}
	if err != nil {
		return err
	}

	// total number of entries stored during the life of this instance.
	EntriesTotal.Increase(1)

	return nil
}

func (dm *DMap) putOnReplicaFragment(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.BACKUP)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	return dm.putOnFragment(e)
}

func (dm *DMap) asyncPutOnBackup(e *env, owner discovery.Member) {
	defer dm.s.wg.Done()

	req := e.toReq(e.replicaOpcode)
	_, err := dm.s.requestTo(owner.String(), req)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
		}
	}
}

func (dm *DMap) asyncPutOnCluster(e *env) error {
	err := dm.putOnFragment(e)
	if err != nil {
		return err
	}

	// Fire and forget mode.
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		if !dm.s.isAlive() {
			return ErrServerGone
		}

		dm.s.wg.Add(1)
		go dm.asyncPutOnBackup(e, owner)
	}

	return nil
}

func (dm *DMap) syncPutOnCluster(e *env) error {
	// Quorum based replication.
	var successful int
	owners := dm.s.backup.PartitionOwnersByHKey(e.hkey)
	for _, owner := range owners {
		req := e.toReq(e.replicaOpcode)
		_, err := dm.s.requestTo(owner.String(), req)
		if err != nil {
			if dm.s.log.V(3).Ok() {
				dm.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", owner, e.dmap, err)
			}
			continue
		}
		successful++
	}
	err := dm.putOnFragment(e)
	if err != nil {
		if dm.s.log.V(3).Ok() {
			dm.s.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", dm.s.rt.This(), e.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= dm.s.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (dm *DMap) setLRUEvictionStats(e *env) error {
	// Try to make room for the new item, if it's required.
	// MaxKeys and MaxInuse properties of LRU can be used in the same time.
	// But I think that it's good to use only one of time in a production system.
	// Because it should be easy to understand and debug.
	st := e.fragment.storage.Stats()
	// This works for every request if you enabled LRU.
	// But loading a number from memory should be very cheap.
	// ownedPartitionCount changes in the case of node join or leave.
	ownedPartitionCount := dm.s.rt.OwnedPartitionCount()
	if dm.config.maxKeys > 0 {
		// MaxKeys controls maximum key count owned by this node.
		// We need ownedPartitionCount property because every partition
		// manages itself independently. So if you set MaxKeys=70 and
		// your partition count is 7, every partition 10 keys at maximum.
		if st.Length >= dm.config.maxKeys/int(ownedPartitionCount) {
			err := dm.evictKeyWithLRU(e)
			if err != nil {
				return err
			}
		}
	}

	if dm.config.maxInuse > 0 {
		// MaxInuse controls maximum in-use memory of partitions on this node.
		// We need ownedPartitionCount property because every partition
		// manages itself independently. So if you set MaxInuse=70M(in bytes) and
		// your partition count is 7, every partition consumes 10M in-use space at maximum.
		// WARNING: Actual allocated memory can be different.
		if st.Inuse >= dm.config.maxInuse/int(ownedPartitionCount) {
			err := dm.evictKeyWithLRU(e)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (dm *DMap) checkPutConditions(e *env) error {
	// Only set the key if it does not already exist.
	if e.flags&IfNotFound != 0 {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if !isKeyExpired(ttl) {
				return ErrKeyFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	// Only set the key if it already exists.
	if e.flags&IfFound != 0 && !e.fragment.storage.Check(e.hkey) {
		ttl, err := e.fragment.storage.GetTTL(e.hkey)
		if err == nil {
			if isKeyExpired(ttl) {
				return ErrKeyNotFound
			}
		}
		if errors.Is(err, storage.ErrKeyNotFound) {
			err = ErrKeyNotFound
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (dm *DMap) putOnCluster(e *env) error {
	part := dm.getPartitionByHKey(e.hkey, partitions.PRIMARY)
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	e.fragment = f
	f.Lock()
	defer f.Unlock()

	if err = dm.checkPutConditions(e); err != nil {
		return err
	}

	if dm.config != nil {
		if dm.config.ttlDuration.Seconds() != 0 && e.timeout.Seconds() == 0 {
			e.timeout = dm.config.ttlDuration
		}
		if dm.config.evictionPolicy == config.LRUEviction {
			if err = dm.setLRUEvictionStats(e); err != nil {
				return err
			}
		}
	}

	if dm.s.config.ReplicaCount > config.MinimumReplicaCount {
		switch dm.s.config.ReplicationMode {
		case config.AsyncReplicationMode:
			// Fire and forget mode. Calls PutBackup command in different goroutines
			// and stores the key/value pair on local storage instance.
			return dm.asyncPutOnCluster(e)
		case config.SyncReplicationMode:
			// Quorum based replication.
			return dm.syncPutOnCluster(e)
		default:
			return fmt.Errorf("invalid replication mode: %v", dm.s.config.ReplicationMode)
		}
	}

	// single replica
	return dm.putOnFragment(e)
}

// put controls every write operation in Olric. It redirects the requests to its owner,
// if the key belongs to another host.
func (dm *DMap) put(e *env) error {
	e.hkey = partitions.HKey(e.dmap, e.key)
	member := dm.s.primary.PartitionByHKey(e.hkey).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		// We are on the partition owner.
		return dm.putOnCluster(e)
	}

	// Redirect to the partition owner.
	req := e.toReq(e.opcode)
	_, err := dm.s.requestTo(member.String(), req)
	return err
}

func (dm *DMap) prepareAndSerialize(opcode protocol.OpCode, key string, value interface{},
	timeout time.Duration, flags int16) (*env, error) {
	val, err := dm.s.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	return newEnv(opcode, dm.name, key, val, timeout, flags, partitions.PRIMARY), nil
}

// PutEx sets the value for the given key with TTL. It overwrites any previous
// value for that key. It's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	e, err := dm.prepareAndSerialize(protocol.OpPutEx, key, value, timeout, 0)
	if err != nil {
		return err
	}
	return dm.put(e)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key and it's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) Put(key string, value interface{}) error {
	e, err := dm.prepareAndSerialize(protocol.OpPut, key, value, nilTimeout, 0)
	if err != nil {
		return err
	}
	return dm.put(e)
}

// PutIf Put sets the value for the given key. It overwrites any previous value
// for that key, and it's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
// Flag argument currently has two different options:
//
// IfNotFound: Only set the key if it does not already exist.
// It returns ErrFound if the key already exist.
//
// IfFound: Only set the key if it already exist.
// It returns ErrKeyNotFound if the key does not exist.
func (dm *DMap) PutIf(key string, value interface{}, flags int16) error {
	e, err := dm.prepareAndSerialize(protocol.OpPutIf, key, value, nilTimeout, flags)
	if err != nil {
		return err
	}
	return dm.put(e)
}

// PutIfEx sets the value for the given key with TTL. It overwrites any previous
// value for that key. It's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
// Flag argument currently has two different options:
//
// IfNotFound: Only set the key if it does not already exist.
// It returns ErrFound if the key already exist.
//
// IfFound: Only set the key if it already exist.
// It returns ErrKeyNotFound if the key does not exist.
func (dm *DMap) PutIfEx(key string, value interface{}, timeout time.Duration, flags int16) error {
	e, err := dm.prepareAndSerialize(protocol.OpPutIfEx, key, value, timeout, flags)
	if err != nil {
		return err
	}
	return dm.put(e)
}
