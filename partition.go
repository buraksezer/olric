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
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/buraksezer/olric/internal/discovery"
)

// partition is a basic, logical storage unit in Olric and stores DMaps in a sync.Map.
type partition struct {
	sync.RWMutex

	id     uint64
	backup bool
	m      sync.Map
	owners atomic.Value
}

// owner returns partition owner. It's not thread-safe.
func (p *partition) owner() discovery.Member {
	if p.backup {
		// programming error. it cannot occur at production!
		panic("cannot call this if backup is true")
	}
	owners := p.owners.Load().([]discovery.Member)
	if len(owners) == 0 {
		panic("owners list cannot be empty")
	}
	return owners[len(owners)-1]
}

// ownerCount returns the current owner count of a partition.
func (p *partition) ownerCount() int {
	owners := p.owners.Load()
	if owners == nil {
		return 0
	}
	return len(owners.([]discovery.Member))
}

// loadOwners loads the partition owners from atomic.value and returns.
func (p *partition) loadOwners() []discovery.Member {
	owners := p.owners.Load()
	if owners == nil {
		return []discovery.Member{}
	}
	return owners.([]discovery.Member)
}

func (p *partition) length() int {
	var length int
	p.m.Range(func(_, dm interface{}) bool {
		d := dm.(*dmap)
		d.RLock()
		defer d.RUnlock()

		length += d.storage.Len()
		// Continue scanning.
		return true
	})
	return length
}

// getPartitionID returns partitionID for a given hkey.
func (db *Olric) getPartitionID(hkey uint64) uint64 {
	return hkey % db.config.PartitionCount
}

// getPartition loads the owner partition for a given hkey.
func (db *Olric) getPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.partitions[partID]
}

// getBackupPartition loads the backup partition for a given hkey.
func (db *Olric) getBackupPartition(hkey uint64) *partition {
	partID := db.getPartitionID(hkey)
	return db.backups[partID]
}

// getBackupOwners returns the backup owners list for a given hkey.
func (db *Olric) getBackupPartitionOwners(hkey uint64) []discovery.Member {
	part := db.getBackupPartition(hkey)
	return part.owners.Load().([]discovery.Member)
}

// getPartitionOwners loads the partition owners list for a given hkey.
func (db *Olric) getPartitionOwners(hkey uint64) []discovery.Member {
	part := db.getPartition(hkey)
	return part.owners.Load().([]discovery.Member)
}

// getHKey returns hash-key, a.k.a hkey, for a key on a dmap.
func (db *Olric) getHKey(name, key string) uint64 {
	tmp := name + key
	return db.hasher.Sum64(*(*[]byte)(unsafe.Pointer(&tmp)))
}

// findPartitionOwner finds the partition owner for a key on a dmap.
func (db *Olric) findPartitionOwner(name, key string) (discovery.Member, uint64) {
	hkey := db.getHKey(name, key)
	return db.getPartition(hkey).owner(), hkey
}
