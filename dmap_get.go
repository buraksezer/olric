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
	"errors"
	"sort"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"github.com/vmihailenco/msgpack"
)

var ErrReadQuorum = errors.New("read quorum cannot be reached")

type version struct {
	host *discovery.Member
	data *storage.VData
}

func (db *Olric) unmarshalValue(rawval []byte) (interface{}, error) {
	var value interface{}
	err := db.serializer.Unmarshal(rawval, &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners.
func (db *Olric) lookupOnOwners(dm *dmap, hkey uint64, name, key string) []*version {
	var versions []*version

	// Check on localhost, the partition owner.
	value, err := dm.storage.Get(hkey)
	if err != nil {
		// still need to use "ver". just log this error.
		if err == storage.ErrKeyNotFound {
			// the requested key can be found on a replica or a previous partition owner.
			if db.log.V(5).Ok() {
				db.log.V(5).Printf(
					"[DEBUG] key: %s, HKey: %d on dmap: %s could not be found on the local storage: %v",
					key, hkey, name, err)
			}
		} else {
			db.log.V(3).Printf("[ERROR] Failed to get key: %s on %s could not be found: %s", key, name, err)
		}
	}

	ver := &version{
		host: &db.this,
		data: value,
	}
	versions = append(versions, ver)

	// Run a query on the previous owners.
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		req := protocol.NewDMapMessage(protocol.OpGetPrev)
		req.SetDMap(name)
		req.SetKey(key)

		ver := &version{host: &owner}
		resp, err := db.requestTo(owner.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call get on a previous "+
					"primary owner: %s: %v", owner, err)
			}
		} else {
			data := storage.VData{}
			err = msgpack.Unmarshal(resp.Value(), data)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to unmarshal data from the "+
					"previous primary owner: %s: %v", owner, err)
			} else {
				ver.data = &data
				// Ignore failed owners. The data on those hosts will be wiped out
				// by the rebalancer.
				versions = append(versions, ver)
			}
		}
	}
	return versions
}

func (db *Olric) sortVersions(versions []*version) []*version {
	sort.Slice(versions,
		func(i, j int) bool {
			return versions[i].data.Timestamp >= versions[j].data.Timestamp
		},
	)
	// Explicit is better than implicit.
	return versions
}

func (db *Olric) sanitizeAndSortVersions(versions []*version) []*version {
	var sanitized []*version
	// We use versions slice for read-repair. Clear nil values first.
	for _, ver := range versions {
		if ver.data != nil {
			sanitized = append(sanitized, ver)
		}
	}
	if len(sanitized) <= 1 {
		return sanitized
	}
	return db.sortVersions(sanitized)
}

func (db *Olric) lookupOnReplicas(hkey uint64, name, key string) []*version {
	var versions []*version
	// Check backups.
	backups := db.getBackupPartitionOwners(hkey)
	for _, replica := range backups {
		req := protocol.NewDMapMessage(protocol.OpGetBackup)
		req.SetDMap(name)
		req.SetKey(key)
		ver := &version{host: &replica}
		resp, err := db.requestTo(replica.String(), req)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call get on a replica owner: %s: %v", replica, err)
			}
		} else {
			value := storage.VData{}
			err = msgpack.Unmarshal(resp.Value(), &value)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to unmarshal data from a replica owner: %s: %v", replica, err)
			} else {
				ver.data = &value
			}
		}
		versions = append(versions, ver)
	}
	return versions
}

func (db *Olric) readRepair(name string, dm *dmap, winner *version, versions []*version) {
	for _, ver := range versions {
		if ver.data != nil && winner.data.Timestamp == ver.data.Timestamp {
			continue
		}

		// If readRepair is enabled, this function is called by every GET request.
		var req *protocol.DMapMessage
		if winner.data.TTL == 0 {
			req = protocol.NewDMapMessage(protocol.OpPutReplica)
			req.SetDMap(name)
			req.SetKey(winner.data.Key)
			req.SetValue(winner.data.Value)
			req.SetExtra(protocol.PutExtra{Timestamp: winner.data.Timestamp})
		} else {
			req := protocol.NewDMapMessage(protocol.OpPutExReplica)
			req.SetDMap(name)
			req.SetKey(winner.data.Key)
			req.SetValue(winner.data.Value)
			req.SetExtra(protocol.PutExExtra{
				Timestamp: winner.data.Timestamp,
				TTL:       winner.data.TTL,
			})
		}

		// Sync
		if hostCmp(*ver.host, db.this) {
			hkey := db.getHKey(name, winner.data.Key)
			w := &writeop{
				dmap:      name,
				key:       winner.data.Key,
				value:     winner.data.Value,
				timestamp: winner.data.Timestamp,
				timeout:   time.Duration(winner.data.TTL),
			}
			dm.Lock()
			err := db.localPut(hkey, dm, w)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to synchronize with replica: %v", err)
			}
			dm.Unlock()
		} else {
			_, err := db.requestTo(ver.host.String(), req)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to synchronize replica %s: %v", ver.host, err)
			}
		}
	}
}

func (db *Olric) callGetOnCluster(hkey uint64, name, key string) ([]byte, error) {
	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return nil, err
	}
	dm.RLock()
	// RUnlock should not be called with defer statement here because
	// readRepair function may call localPut function which needs a write
	// lock. Please don't forget calling RUnlock before returning here.

	versions := db.lookupOnOwners(dm, hkey, name, key)
	if db.config.ReadQuorum >= config.MinimumReplicaCount {
		v := db.lookupOnReplicas(hkey, name, key)
		versions = append(versions, v...)
	}
	if len(versions) < db.config.ReadQuorum {
		dm.RUnlock()
		return nil, ErrReadQuorum
	}
	sorted := db.sanitizeAndSortVersions(versions)
	if len(sorted) == 0 {
		// We checked everywhere, it's not here.
		dm.RUnlock()
		return nil, ErrKeyNotFound
	}
	if len(sorted) < db.config.ReadQuorum {
		dm.RUnlock()
		return nil, ErrReadQuorum
	}

	// The most up-to-date version of the values.
	winner := sorted[0]
	if isKeyExpired(winner.data.TTL) || dm.isKeyIdle(hkey) {
		dm.RUnlock()
		return nil, ErrKeyNotFound
	}
	// LRU and MaxIdleDuration eviction policies are only valid on
	// the partition owner. Normally, we shouldn't need to retrieve the keys
	// from the backup or the previous owners. When the fsck merge
	// a fragmented partition or recover keys from a backup, Olric
	// continue maintaining a reliable access log.
	dm.updateAccessLog(hkey)

	dm.RUnlock()
	if db.config.ReadRepair {
		// Parallel read operations may propagate different versions of
		// the same key/value pair. The rule is simple: last write wins.
		db.readRepair(name, dm, winner, versions)
	}
	return winner.data.Value, nil
}

func (db *Olric) get(name, key string) ([]byte, error) {
	member, hkey := db.findPartitionOwner(name, key)
	// We are on the partition owner
	if hostCmp(member, db.this) {
		return db.callGetOnCluster(hkey, name, key)
	}

	// Redirect to the partition owner
	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetDMap(name)
	req.SetKey(key)
	resp, err := db.requestTo(member.String(), req)
	if err != nil {
		return nil, err
	}
	return resp.Value(), nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value. It is safe to modify the contents of the argument
// after Get returns.
func (dm *DMap) Get(key string) (interface{}, error) {
	rawval, err := dm.db.get(dm.name, key)
	if err != nil {
		return nil, err
	}
	return dm.db.unmarshalValue(rawval)
}

func (db *Olric) exGetOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	value, err := db.get(req.DMap(), req.Key())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (db *Olric) getBackupOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	dm, err := db.getBackupDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.RLock()
	defer dm.RUnlock()
	vdata, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	if isKeyExpired(vdata.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}

	value, err := msgpack.Marshal(*vdata)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (db *Olric) getPrevOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	part := db.getPartition(hkey)
	tmp, ok := part.m.Load(req.DMap())
	if !ok {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	dm := tmp.(*dmap)

	vdata, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	if isKeyExpired(vdata.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}

	value, err := msgpack.Marshal(*vdata)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}
