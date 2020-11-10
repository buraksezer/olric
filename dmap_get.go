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
)

// Entry is a DMap entry with its metadata.
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}

var ErrReadQuorum = errors.New("read quorum cannot be reached")

type version struct {
	host  *discovery.Member
	entry *storage.Entry
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

func (db *Olric) lookupOnPreviousOwner(owner *discovery.Member, name, key string) (*version, error) {
	req := protocol.NewDMapMessage(protocol.OpGetPrev)
	req.SetDMap(name)
	req.SetKey(key)

	v := &version{host: owner}
	resp, err := db.requestTo(owner.String(), req)
	if err != nil {
		return nil, err
	}
	data := storage.NewEntry()
	data.Decode(resp.Value())
	v.entry = data
	return v, nil
}

func (db *Olric) lookupOnThisNode(dm *dmap, hkey uint64, name, key string) *version {
	// Check on localhost, the partition owner.
	value, err := dm.storage.Get(hkey)
	if err != nil {
		// still need to use "ver". just log this error.
		if err == storage.ErrKeyNotFound {
			// the requested key can be found on a replica or a previous partition owner.
			if db.log.V(5).Ok() {
				db.log.V(5).Printf(
					"[DEBUG] key: %s, HKey: %d on DMap: %s could not be found on the local storage: %v",
					key, hkey, name, err)
			}
		} else {
			db.log.V(3).Printf("[ERROR] Failed to get key: %s on %s could not be found: %s", key, name, err)
		}
	}
	return &version{
		host:  &db.this,
		entry: value,
	}
}

// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners.
func (db *Olric) lookupOnOwners(dm *dmap, hkey uint64, name, key string) []*version {
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	var versions []*version
	versions = append(versions, db.lookupOnThisNode(dm, hkey, name, key))

	// Run a query on the previous owners.
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		v, err := db.lookupOnPreviousOwner(&owner, name, key)
		if err != nil {
			if db.log.V(3).Ok() {
				db.log.V(3).Printf("[ERROR] Failed to call get on a previous "+
					"primary owner: %s: %v", owner, err)
			}
			continue
		}
		// Ignore failed owners. The data on those hosts will be wiped out
		// by the rebalancer.
		versions = append(versions, v)
	}
	return versions
}

func (db *Olric) sortVersions(versions []*version) []*version {
	sort.Slice(versions,
		func(i, j int) bool {
			return versions[i].entry.Timestamp >= versions[j].entry.Timestamp
		},
	)
	// Explicit is better than implicit.
	return versions
}

func (db *Olric) sanitizeAndSortVersions(versions []*version) []*version {
	var sanitized []*version
	// We use versions slice for read-repair. Clear nil values first.
	for _, ver := range versions {
		if ver.entry != nil {
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
			data := storage.NewEntry()
			data.Decode(resp.Value())
			ver.entry = data
		}
		versions = append(versions, ver)
	}
	return versions
}

func (db *Olric) readRepair(name string, dm *dmap, winner *version, versions []*version) {
	for _, ver := range versions {
		if ver.entry != nil && winner.entry.Timestamp == ver.entry.Timestamp {
			continue
		}

		// If readRepair is enabled, this function is called by every GET request.
		var req *protocol.DMapMessage
		if winner.entry.TTL == 0 {
			req = protocol.NewDMapMessage(protocol.OpPutReplica)
			req.SetDMap(name)
			req.SetKey(winner.entry.Key)
			req.SetValue(winner.entry.Value)
			req.SetExtra(protocol.PutExtra{Timestamp: winner.entry.Timestamp})
		} else {
			req = protocol.NewDMapMessage(protocol.OpPutExReplica)
			req.SetDMap(name)
			req.SetKey(winner.entry.Key)
			req.SetValue(winner.entry.Value)
			req.SetExtra(protocol.PutExExtra{
				Timestamp: winner.entry.Timestamp,
				TTL:       winner.entry.TTL,
			})
		}

		// Sync
		if cmpMembersByID(*ver.host, db.this) {
			hkey := db.getHKey(name, winner.entry.Key)
			w := &writeop{
				dmap:      name,
				key:       winner.entry.Key,
				value:     winner.entry.Value,
				timestamp: winner.entry.Timestamp,
				timeout:   time.Duration(winner.entry.TTL),
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

func (db *Olric) callGetOnCluster(hkey uint64, name, key string) (*storage.Entry, error) {
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
	if isKeyExpired(winner.entry.TTL) || dm.isKeyIdle(hkey) {
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
	return winner.entry, nil
}

func (db *Olric) get(name, key string) (*storage.Entry, error) {
	member, hkey := db.findPartitionOwner(name, key)
	// We are on the partition owner
	if cmpMembersByName(member, db.this) {
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
	entry := storage.NewEntry()
	entry.Decode(resp.Value())
	return entry, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) Get(key string) (interface{}, error) {
	rawval, err := dm.db.get(dm.name, key)
	if err != nil {
		return nil, err
	}
	return dm.db.unmarshalValue(rawval.Value)
}

// GetEntry gets the value for the given key with its metadata. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) GetEntry(key string) (*Entry, error) {
	entry, err := dm.db.get(dm.name, key)
	if err != nil {
		return nil, err
	}
	value, err := dm.db.unmarshalValue(entry.Value)
	if err != nil {
		return nil, err
	}
	return &Entry{
		Key:       entry.Key,
		Value:     value,
		TTL:       entry.TTL,
		Timestamp: entry.Timestamp,
	}, nil
}

func (db *Olric) exGetOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	entry, err := db.get(req.DMap(), req.Key())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
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
	entry, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	if isKeyExpired(entry.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
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

	entry, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	if isKeyExpired(entry.TTL) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}
