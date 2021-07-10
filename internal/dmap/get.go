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
	"sort"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/stats"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/pkg/storage"
)

// Entry is a DMap entry with its metadata.
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}

var (
	// GetMisses is the number of entries that have been requested and not found
	GetMisses = stats.NewInt64Counter()

	// GetHits is the number of entries that have been requested and found present
	GetHits = stats.NewInt64Counter()

	// EvictedTotal is the number of entries removed from cache to free memory for new entries.
	EvictedTotal = stats.NewInt64Counter()
)

var ErrReadQuorum = neterrors.New(protocol.StatusErrReadQuorum, "read quorum cannot be reached")

type version struct {
	host  *discovery.Member
	entry storage.Entry
}

func (dm *DMap) unmarshalValue(raw []byte) (interface{}, error) {
	var value interface{}
	err := dm.s.serializer.Unmarshal(raw, &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

func (dm *DMap) getOnFragment(e *env) (storage.Entry, error) {
	part := dm.getPartitionByHKey(e.hkey, e.kind)
	f, err := dm.loadFragment(part)
	if err != nil {
		return nil, err
	}

	f.RLock()
	defer f.RUnlock()

	entry, err := f.storage.Get(e.hkey)
	if err != nil {
		return nil, err
	}

	if isKeyExpired(entry.TTL()) {
		return nil, ErrKeyNotFound
	}
	return entry, nil
}

func (dm *DMap) lookupOnPreviousOwner(owner *discovery.Member, key string) (*version, error) {
	req := protocol.NewDMapMessage(protocol.OpGetPrev)
	req.SetDMap(dm.name)
	req.SetKey(key)

	v := &version{host: owner}
	resp, err := dm.s.requestTo(owner.String(), req)
	if err != nil {
		return nil, err
	}
	data := dm.engine.NewEntry()
	data.Decode(resp.Value())
	v.entry = data
	return v, nil
}

func (dm *DMap) valueToVersion(value storage.Entry) *version {
	this := dm.s.rt.This()
	return &version{
		host:  &this,
		entry: value,
	}
}

func (dm *DMap) lookupOnThisNode(hkey uint64, key string) *version {
	// Check on localhost, the partition owner.
	part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
	f, err := dm.loadFragment(part)
	if err != nil {
		if !errors.Is(err, errFragmentNotFound) {
			dm.s.log.V(3).Printf("[ERROR] Failed to get DMap fragment: %v", err)
		}
		return dm.valueToVersion(nil)
	}
	f.RLock()
	defer f.RUnlock()
	value, err := f.storage.Get(hkey)
	if err != nil {
		if !errors.Is(err, storage.ErrKeyNotFound) {
			// still need to use "ver". just log this error.
			dm.s.log.V(3).Printf("[ERROR] Failed to get key: %s on %s: %s", key, dm.name, err)
		}
		return dm.valueToVersion(nil)
	}
	// We found the key
	//
	// LRU and MaxIdleDuration eviction policies are only valid on
	// the partition owner. Normally, we shouldn't need to retrieve the keys
	// from the backup or the previous owners. When the fsck merge
	// a fragmented partition or recover keys from a backup, Olric
	// continue maintaining a reliable access log.
	dm.updateAccessLog(hkey, f)
	return dm.valueToVersion(value)
}

// lookupOnOwners collects versions of a key/value pair on the partition owner
// by including previous partition owners.
func (dm *DMap) lookupOnOwners(hkey uint64, key string) []*version {
	owners := dm.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	var versions []*version
	versions = append(versions, dm.lookupOnThisNode(hkey, key))

	// Run a query on the previous owners.
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		v, err := dm.lookupOnPreviousOwner(&owner, key)
		if err != nil {
			if dm.s.log.V(3).Ok() {
				dm.s.log.V(3).Printf("[ERROR] Failed to call get on a previous "+
					"primary owner: %s: %v", owner, err)
			}
			continue
		}
		// Ignore failed owners. The data on those hosts will be wiped out
		// by the balancer.
		versions = append(versions, v)
	}
	return versions
}

func (dm *DMap) sortVersions(versions []*version) []*version {
	sort.Slice(versions,
		func(i, j int) bool {
			return versions[i].entry.Timestamp() >= versions[j].entry.Timestamp()
		},
	)
	// Explicit is better than implicit.
	return versions
}

func (dm *DMap) sanitizeAndSortVersions(versions []*version) []*version {
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
	return dm.sortVersions(sanitized)
}

func (dm *DMap) lookupOnReplicas(hkey uint64, key string) []*version {
	// Check backup.
	backups := dm.s.backup.PartitionOwnersByHKey(hkey)
	versions := make([]*version, 0, len(backups))
	for _, replica := range backups {
		req := protocol.NewDMapMessage(protocol.OpGetReplica)
		req.SetDMap(dm.name)
		req.SetKey(key)
		host := replica
		v := &version{host: &host}
		resp, err := dm.s.requestTo(replica.String(), req)
		if err != nil {
			if dm.s.log.V(6).Ok() {
				dm.s.log.V(6).Printf("[ERROR] Failed to call get on"+
					" a replica owner: %s: %v", replica, err)
			}
		} else {
			data := dm.engine.NewEntry()
			data.Decode(resp.Value())
			v.entry = data
		}
		versions = append(versions, v)
	}
	return versions
}

func (dm *DMap) readRepair(winner *version, versions []*version) {
	for _, version := range versions {
		if version.entry != nil && winner.entry.Timestamp() == version.entry.Timestamp() {
			continue
		}

		// Sync
		tmp := *version.host
		if tmp.CompareByID(dm.s.rt.This()) {
			hkey := partitions.HKey(dm.name, winner.entry.Key())
			part := dm.getPartitionByHKey(hkey, partitions.PRIMARY)
			f, err := dm.loadOrCreateFragment(part)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to get or create the fragment for: %s on %s: %v",
					winner.entry.Key(), dm.name, err)
				return
			}

			f.Lock()
			e := &env{
				dmap:      dm.name,
				key:       winner.entry.Key(),
				value:     winner.entry.Value(),
				timestamp: winner.entry.Timestamp(),
				timeout:   time.Duration(winner.entry.TTL()),
				hkey:      hkey,
				fragment:  f,
			}
			err = dm.putOnFragment(e)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to synchronize with replica: %v", err)
			}
			f.Unlock()
		} else {
			// If readRepair is enabled, this function is called by every GET request.
			var req *protocol.DMapMessage
			if winner.entry.TTL() == 0 {
				req = protocol.NewDMapMessage(protocol.OpPutReplica)
				req.SetDMap(dm.name)
				req.SetKey(winner.entry.Key())
				req.SetValue(winner.entry.Value())
				req.SetExtra(protocol.PutExtra{Timestamp: winner.entry.Timestamp()})
			} else {
				req = protocol.NewDMapMessage(protocol.OpPutExReplica)
				req.SetDMap(dm.name)
				req.SetKey(winner.entry.Key())
				req.SetValue(winner.entry.Value())
				req.SetExtra(protocol.PutExExtra{
					Timestamp: winner.entry.Timestamp(),
					TTL:       winner.entry.TTL(),
				})
			}
			_, err := dm.s.requestTo(version.host.String(), req)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to synchronize replica %s: %v", version.host, err)
			}
		}
	}
}

func (dm *DMap) getOnCluster(hkey uint64, key string) (storage.Entry, error) {
	// RUnlock should not be called with defer statement here because
	// readRepair function may call putOnFragment function which needs a write
	// lock. Please don't forget calling RUnlock before returning here.
	versions := dm.lookupOnOwners(hkey, key)
	if dm.s.config.ReadQuorum >= config.MinimumReplicaCount {
		v := dm.lookupOnReplicas(hkey, key)
		versions = append(versions, v...)
	}
	if len(versions) < dm.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}
	sorted := dm.sanitizeAndSortVersions(versions)
	if len(sorted) == 0 {
		// We checked everywhere, it's not here.
		return nil, ErrKeyNotFound
	}
	if len(sorted) < dm.s.config.ReadQuorum {
		return nil, ErrReadQuorum
	}

	// The most up-to-date version of the values.
	winner := sorted[0]
	if isKeyExpired(winner.entry.TTL()) || dm.isKeyIdle(hkey) {
		return nil, ErrKeyNotFound
	}

	if dm.s.config.ReadRepair {
		// Parallel read operations may propagate different versions of
		// the same key/value pair. The rule is simple: last write wins.
		dm.readRepair(winner, versions)
	}
	return winner.entry, nil
}

func (dm *DMap) get(key string) (storage.Entry, error) {
	hkey := partitions.HKey(dm.name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	// We are on the partition owner
	if member.CompareByName(dm.s.rt.This()) {
		entry, err := dm.getOnCluster(hkey, key)
		if errors.Is(err, ErrKeyNotFound) {
			GetMisses.Increase(1)
		}
		if err != nil {
			return nil, err
		}

		// number of keys that have been requested and found present
		GetHits.Increase(1)

		return entry, nil
	}

	// Redirect to the partition owner
	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetDMap(dm.name)
	req.SetKey(key)

	resp, err := dm.s.requestTo(member.String(), req)
	if errors.Is(err, ErrKeyNotFound) {
		GetMisses.Increase(1)
	}
	if err != nil {
		return nil, err
	}

	// number of keys that have been requested and found present
	GetHits.Increase(1)

	entry := dm.engine.NewEntry()
	entry.Decode(resp.Value())
	return entry, nil
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) Get(key string) (interface{}, error) {
	raw, err := dm.get(key)
	if err != nil {
		return nil, err
	}
	return dm.unmarshalValue(raw.Value())
}

// GetEntry gets the value for the given key with its metadata. It returns ErrKeyNotFound if the DB
// does not contains the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) GetEntry(key string) (*Entry, error) {
	entry, err := dm.get(key)
	if err != nil {
		return nil, err
	}
	value, err := dm.unmarshalValue(entry.Value())
	if err != nil {
		return nil, err
	}
	return &Entry{
		Key:       entry.Key(),
		Value:     value,
		TTL:       entry.TTL(),
		Timestamp: entry.Timestamp(),
	}, nil
}
