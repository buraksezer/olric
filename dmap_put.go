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

package olric

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/pkg/errors"
)

const (
	IfNotFound = int16(1) << iota
	IfFound
)

var (
	ErrKeyFound    = errors.New("key found")
	ErrWriteQuorum = errors.New("write quorum cannot be reached")
)

// writeop contains various values whose participate a write operation.
type writeop struct {
	opcode        protocol.OpCode
	replicaOpcode protocol.OpCode
	dmap          string
	key           string
	value         []byte
	timestamp     int64
	timeout       time.Duration
	flags         int16
}

// fromReq generates a new protocol message from writeop instance.
func (w *writeop) fromReq(r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	w.dmap = req.DMap()
	w.key = req.Key()
	w.value = req.Value()
	w.opcode = req.Op

	// Set opcode for a possible replica operation
	switch w.opcode {
	case protocol.OpPut:
		w.replicaOpcode = protocol.OpPutReplica
	case protocol.OpPutEx:
		w.replicaOpcode = protocol.OpPutExReplica
	case protocol.OpPutIf:
		w.replicaOpcode = protocol.OpPutIfReplica
	case protocol.OpPutIfEx:
		w.replicaOpcode = protocol.OpPutIfExReplica
	}

	// Extract extras
	switch req.Op {
	case protocol.OpPut, protocol.OpPutReplica:
		w.timestamp = req.Extra().(protocol.PutExtra).Timestamp
	case protocol.OpPutEx, protocol.OpPutExReplica:
		w.timestamp = req.Extra().(protocol.PutExExtra).Timestamp
		w.timeout = time.Duration(req.Extra().(protocol.PutExExtra).TTL)
	case protocol.OpPutIf, protocol.OpPutIfReplica:
		w.flags = req.Extra().(protocol.PutIfExtra).Flags
		w.timestamp = req.Extra().(protocol.PutIfExtra).Timestamp
	case protocol.OpPutIfEx, protocol.OpPutIfExReplica:
		w.flags = req.Extra().(protocol.PutIfExExtra).Flags
		w.timestamp = req.Extra().(protocol.PutIfExExtra).Timestamp
		w.timeout = time.Duration(req.Extra().(protocol.PutIfExExtra).TTL)
	case protocol.OpExpire:
		w.timestamp = req.Extra().(protocol.ExpireExtra).Timestamp
		w.timeout = time.Duration(req.Extra().(protocol.ExpireExtra).TTL)
	}
}

// toReq generates a new protocol message from a writeop.
func (w *writeop) toReq(opcode protocol.OpCode) *protocol.DMapMessage {
	req := protocol.NewDMapMessage(opcode)
	req.SetDMap(w.dmap)
	req.SetKey(w.key)
	req.SetValue(w.value)

	// Prepare extras
	switch opcode {
	case protocol.OpPut, protocol.OpPutReplica:
		req.SetExtra(protocol.PutExtra{
			Timestamp: w.timestamp,
		})
	case protocol.OpPutEx, protocol.OpPutExReplica:
		req.SetExtra(protocol.PutExExtra{
			TTL:       w.timeout.Nanoseconds(),
			Timestamp: w.timestamp,
		})
	case protocol.OpPutIf, protocol.OpPutIfReplica:
		req.SetExtra(protocol.PutIfExtra{
			Flags:     w.flags,
			Timestamp: w.timestamp,
		})
	case protocol.OpPutIfEx, protocol.OpPutIfExReplica:
		req.SetExtra(protocol.PutIfExExtra{
			Flags:     w.flags,
			Timestamp: w.timestamp,
			TTL:       w.timeout.Nanoseconds(),
		})
	case protocol.OpExpire:
		req.SetExtra(protocol.ExpireExtra{
			Timestamp: w.timestamp,
			TTL:       w.timeout.Nanoseconds(),
		})
	}
	return req
}

// localPut calls underlying storage engine's Put method to store the key/value pair.
func (db *Olric) localPut(hkey uint64, dm *dmap, w *writeop) error {
	var ttl int64
	if w.timeout.Seconds() != 0 {
		ttl = getTTL(w.timeout)
	}
	val := &storage.Entry{
		Key:       w.key,
		Value:     w.value,
		Timestamp: w.timestamp,
		TTL:       ttl,
	}
	err := dm.storage.Put(hkey, val)
	if err == storage.ErrFragmented {
		db.wg.Add(1)
		go db.compactTables(dm)
		err = nil
	}
	if err == nil {
		dm.updateAccessLog(hkey)
		return nil
	}
	return err
}

func (db *Olric) asyncPutOnCluster(hkey uint64, dm *dmap, w *writeop) error {
	// Fire and forget mode.
	owners := db.getBackupPartitionOwners(hkey)
	for _, owner := range owners {
		db.wg.Add(1)
		go func(host discovery.Member) {
			defer db.wg.Done()
			req := w.toReq(w.replicaOpcode)
			_, err := db.requestTo(host.String(), req)
			if err != nil {
				if db.log.V(3).Ok() {
					db.log.V(3).Printf("[ERROR] Failed to create replica in async mode: %v", err)
				}
			}
		}(owner)
	}
	return db.localPut(hkey, dm, w)
}

func (db *Olric) syncPutOnCluster(hkey uint64, dm *dmap, w *writeop) error {
	// Quorum based replication.
	var successful int
	owners := db.getBackupPartitionOwners(hkey)
	for _, owner := range owners {
		req := w.toReq(w.replicaOpcode)
		_, err := db.requestTo(owner.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", owner, w.dmap, err)
			}
			continue
		}
		successful++
	}
	err := db.localPut(hkey, dm, w)
	if err != nil {
		if db.log.V(3).Ok() {
			db.log.V(3).Printf("[ERROR] Failed to call put command on %s for DMap: %s: %v", db.this, w.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= db.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (db *Olric) callPutOnCluster(hkey uint64, w *writeop) error {
	// Get the dmap and acquire its lock
	dm, err := db.getDMap(w.dmap, hkey)
	if err != nil {
		return err
	}
	dm.Lock()
	defer dm.Unlock()

	// Only set the key if it does not already exist.
	if w.flags&IfNotFound != 0 {
		ttl, err := dm.storage.GetTTL(hkey)
		if err == nil {
			if !isKeyExpired(ttl) {
				return ErrKeyFound
			}
		}
		if err == storage.ErrKeyNotFound {
			err = nil
		}
		if err != nil {
			return err
		}
	}

	// Only set the key if it already exist.
	if w.flags&IfFound != 0 && !dm.storage.Check(hkey) {
		ttl, err := dm.storage.GetTTL(hkey)
		if err == nil {
			if isKeyExpired(ttl) {
				return ErrKeyNotFound
			}
		}
		if err == storage.ErrKeyNotFound {
			err = ErrKeyNotFound
		}
		if err != nil {
			return err
		}
	}

	// MaxKeys and MaxInuse properties of LRU can be used in the same time.
	// But I think that it's good to use only one of time in a production system.
	// Because it should be easy to understand and debug.

	// Try to make room for the new item, if it's required.
	if dm.cache != nil && dm.cache.evictionPolicy == config.LRUEviction {
		// This works for every request if you enabled LRU.
		// But loading a number from memory should be very cheap.
		// ownedPartitionCount changes in the case of node join or leave.
		ownedPartitionCount := atomic.LoadUint64(&db.ownedPartitionCount)

		if dm.cache.maxKeys > 0 {
			// MaxKeys controls maximum key count owned by this node.
			// We need ownedPartitionCount property because every partition
			// manages itself independently. So if you set MaxKeys=70 and
			// your partition count is 7, every partition 10 keys at maximum.
			if dm.storage.Len() >= dm.cache.maxKeys/int(ownedPartitionCount) {
				err := db.evictKeyWithLRU(dm, w.dmap)
				if err != nil {
					return err
				}
			}
		}

		if dm.cache.maxInuse > 0 {
			// MaxInuse controls maximum in-use memory of partitions on this node.
			// We need ownedPartitionCount property because every partition
			// manages itself independently. So if you set MaxInuse=70M(in bytes) and
			// your partition count is 7, every partition consumes 10M in-use space at maximum.
			// WARNING: Actual allocated memory can be different.
			if dm.storage.Inuse() >= dm.cache.maxInuse/int(ownedPartitionCount) {
				err := db.evictKeyWithLRU(dm, w.dmap)
				if err != nil {
					return err
				}
			}
		}
	}

	if dm.cache != nil && dm.cache.ttlDuration.Seconds() != 0 && w.timeout.Seconds() == 0 {
		w.timeout = dm.cache.ttlDuration
	}

	if db.config.ReplicaCount == config.MinimumReplicaCount {
		// MinimumReplicaCount is 1. So it's enough to put the key locally. There is no
		// other replica host.
		return db.localPut(hkey, dm, w)
	}

	if db.config.ReplicationMode == config.AsyncReplicationMode {
		// Fire and forget mode. Calls PutBackup command in different goroutines
		// and stores the key/value pair on local storage instance.
		return db.asyncPutOnCluster(hkey, dm, w)
	} else if db.config.ReplicationMode == config.SyncReplicationMode {
		// Quorum based replication.
		return db.syncPutOnCluster(hkey, dm, w)
	}
	return fmt.Errorf("invalid replication mode: %v", db.config.ReplicationMode)
}

// put controls every write operation in Olric. It redirects the requests to its owner,
// if the key belongs to another host.
func (db *Olric) put(w *writeop) error {
	member, hkey := db.findPartitionOwner(w.dmap, w.key)
	if cmpMembersByName(member, db.this) {
		// We are on the partition owner.
		return db.callPutOnCluster(hkey, w)
	}
	// Redirect to the partition owner.
	req := w.toReq(w.opcode)
	_, err := db.requestTo(member.String(), req)
	return err
}

func (db *Olric) prepareWriteop(opcode protocol.OpCode, name, key string,
	value interface{}, timeout time.Duration, flags int16) (*writeop, error) {
	val, err := db.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	w := &writeop{
		opcode:    opcode,
		dmap:      name,
		key:       key,
		value:     val,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
		flags:     flags,
	}
	switch {
	case opcode == protocol.OpPut:
		w.replicaOpcode = protocol.OpPutReplica
	case opcode == protocol.OpPutEx:
		w.replicaOpcode = protocol.OpPutExReplica
	case opcode == protocol.OpPutIf:
		w.replicaOpcode = protocol.OpPutIfReplica
	case opcode == protocol.OpPutIfEx:
		w.replicaOpcode = protocol.OpPutIfExReplica
	}
	return w, nil
}

// PutEx sets the value for the given key with TTL. It overwrites any previous
// value for that key. It's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	w, err := dm.db.prepareWriteop(protocol.OpPutEx, dm.name, key, value, timeout, 0)
	if err != nil {
		return err
	}
	return dm.db.put(w)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key and it's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) Put(key string, value interface{}) error {
	w, err := dm.db.prepareWriteop(protocol.OpPut, dm.name, key, value, nilTimeout, 0)
	if err != nil {
		return err
	}
	return dm.db.put(w)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key and it's thread-safe. The key has to be string. value type
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
	w, err := dm.db.prepareWriteop(protocol.OpPutIf, dm.name, key, value, nilTimeout, flags)
	if err != nil {
		return err
	}
	return dm.db.put(w)
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
	w, err := dm.db.prepareWriteop(protocol.OpPutIfEx, dm.name, key, value, timeout, flags)
	if err != nil {
		return err
	}
	return dm.db.put(w)
}

func (db *Olric) exPutOperation(w, r protocol.EncodeDecoder) {
	wo := &writeop{}
	wo.fromReq(r)
	err := db.put(wo)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) putReplicaOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	dm, err := db.getBackupDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.Lock()
	defer dm.Unlock()

	wo := &writeop{}
	wo.fromReq(req)
	err = db.localPut(hkey, dm, wo)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) compactTables(dm *dmap) {
	defer db.wg.Done()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(50 * time.Millisecond)
		select {
		case <-timer.C:
			dm.Lock()
			if done := dm.storage.CompactTables(); done {
				// Fragmented tables are merged. Quit.
				dm.Unlock()
				return
			}
			dm.Unlock()
		case <-db.ctx.Done():
			return
		}
	}
}
