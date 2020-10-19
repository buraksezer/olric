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
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
)

func (db *Olric) localExpire(hkey uint64, dm *dmap, w *writeop) error {
	ttl := getTTL(w.timeout)
	val := &storage.Entry{
		Timestamp: w.timestamp,
		TTL:       ttl,
	}
	err := dm.storage.UpdateTTL(hkey, val)
	if err != nil {
		if err == storage.ErrKeyNotFound {
			err = ErrKeyNotFound
		}
		return err
	}
	dm.updateAccessLog(hkey)
	return nil
}

func (db *Olric) asyncExpireOnCluster(hkey uint64, dm *dmap, w *writeop) error {
	req := w.toReq(protocol.OpExpireReplica)
	// Fire and forget mode.
	owners := db.getBackupPartitionOwners(hkey)
	for _, owner := range owners {
		db.wg.Add(1)
		go func(host discovery.Member) {
			defer db.wg.Done()
			_, err := db.requestTo(host.String(), req)
			if err != nil {
				if db.log.V(3).Ok() {
					db.log.V(3).Printf("[ERROR] Failed to set expire in async mode: %v", err)
				}
			}
		}(owner)
	}
	return db.localPut(hkey, dm, w)
}

func (db *Olric) syncExpireOnCluster(hkey uint64, dm *dmap, w *writeop) error {
	req := w.toReq(protocol.OpExpireReplica)

	// Quorum based replication.
	var successful int
	owners := db.getBackupPartitionOwners(hkey)
	for _, owner := range owners {
		_, err := db.requestTo(owner.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
					owner, w.dmap, err)
			}
			continue
		}
		successful++
	}
	err := db.localExpire(hkey, dm, w)
	if err != nil {
		if db.log.V(3).Ok() {
			db.log.V(3).Printf("[ERROR] Failed to call expire command on %s for DMap: %s: %v",
				db.this, w.dmap, err)
		}
	} else {
		successful++
	}
	if successful >= db.config.WriteQuorum {
		return nil
	}
	return ErrWriteQuorum
}

func (db *Olric) callExpireOnCluster(hkey uint64, w *writeop) error {
	// Get the dmap and acquire its lock
	dm, err := db.getDMap(w.dmap, hkey)
	if err != nil {
		return err
	}
	dm.Lock()
	defer dm.Unlock()

	if db.config.ReplicaCount == config.MinimumReplicaCount {
		// MinimumReplicaCount is 1. So it's enough to put the key locally. There is no
		// other replica host.
		return db.localExpire(hkey, dm, w)
	}

	if db.config.ReplicationMode == config.AsyncReplicationMode {
		return db.asyncExpireOnCluster(hkey, dm, w)
	} else if db.config.ReplicationMode == config.SyncReplicationMode {
		return db.syncExpireOnCluster(hkey, dm, w)
	}

	return fmt.Errorf("invalid replication mode: %v", db.config.ReplicationMode)
}

func (db *Olric) expire(w *writeop) error {
	member, hkey := db.findPartitionOwner(w.dmap, w.key)
	if cmpMembersByName(member, db.this) {
		// We are on the partition owner.
		return db.callExpireOnCluster(hkey, w)
	}
	// Redirect to the partition owner
	req := w.toReq(protocol.OpExpire)
	_, err := db.requestTo(member.String(), req)
	return err
}

func (db *Olric) expireReplicaOperation(w, r protocol.EncodeDecoder) {
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
	err = db.localExpire(hkey, dm, wo)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) exExpireOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	wo := &writeop{}
	wo.fromReq(req)
	err := db.expire(wo)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (dm *DMap) Expire(key string, timeout time.Duration) error {
	w := &writeop{
		dmap:      dm.name,
		key:       key,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
	}
	return dm.db.expire(w)
}
