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

	"github.com/buraksezer/olric/internal/storage"
)

// dmap defines the internal representation of a DMap.
type dmap struct {
	sync.RWMutex

	cache   *cache
	storage *storage.Storage
}

// DMap represents a distributed map instance.
type DMap struct {
	name string
	db   *Olric
	hooks map[int8][]HookHandler
}

// NewDMap creates an returns a new DMap instance.
func (db *Olric) NewDMap(name string) (*DMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := db.checkOperationStatus(); err != nil {
		return nil, err
	}
	return &DMap{
		name: name,
		db:   db,
		hooks: make(map[int8][]HookHandler),
	}, nil
}

// createDMap creates and returns a new dmap, internal representation of a DMap.
func (db *Olric) createDMap(part *partition, name string, str *storage.Storage) (*dmap, error) {
	// We need to protect storage.New
	part.Lock()
	defer part.Unlock()

	// Try to load one more time. Another goroutine may have created the dmap.
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}

	// create a new map here.
	nm := &dmap{
		storage: str,
	}

	if db.config.Cache != nil {
		err := db.setCacheConfiguration(nm, name)
		if err != nil {
			return nil, err
		}
	}

	// rebalancer code may send a storage instance for the new DMap. Just use it.
	if nm.storage != nil {
		nm.storage = str
	} else {
		nm.storage = storage.New(db.config.TableSize)
	}

	part.m.Store(name, nm)
	return nm, nil
}

func (db *Olric) getOrCreateDMap(part *partition, name string) (*dmap, error) {
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name, nil)
}

// getDMap loads or creates a dmap.
func (db *Olric) getDMap(name string, hkey uint64) (*dmap, error) {
	part := db.getPartition(hkey)
	return db.getOrCreateDMap(part, name)
}

func (db *Olric) getBackupDMap(name string, hkey uint64) (*dmap, error) {
	part := db.getBackupPartition(hkey)
	dm, ok := part.m.Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name, nil)
}
