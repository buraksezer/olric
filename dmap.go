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
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/storage"
)

// dmap defines the internal representation of a dmap.
type dmap struct {
	sync.RWMutex

	config  *dmapConfig
	client  *transport.Client
	storage storage.Engine
}

func (dm *dmap) Name() string {
	return "DMap"
}

func (dm *dmap) Length() int {
	dm.RLock()
	defer dm.RUnlock()

	return dm.storage.Stats().Length
}

var _ partitions.StorageUnit = (*dmap)(nil)

// dmap represents a distributed map instance.
type DMap struct {
	name string
	db   *Olric
}

// NewDMap creates an returns a new dmap instance.
func (db *Olric) NewDMap(name string) (*DMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := db.isOperable(); err != nil {
		return nil, err
	}
	return &DMap{
		name: name,
		db:   db,
	}, nil
}

// createDMap creates and returns a new dmap, internal representation of a dmap. This function is not thread-safe.
func (db *Olric) createDMap(part *partitions.Partition, name string) (*dmap, error) {
	// create a new map here.
	nm := &dmap{
		client: db.client,
		config: &dmapConfig{
			storageEngine: config.DefaultStorageEngine,
		},
	}
	if db.config.DMaps != nil {
		err := db.setDMapConfiguration(nm, name)
		if err != nil {
			return nil, err
		}
	}
	var err error
	// rebalancer code may send a storage instance for the new dmap. Just use it.
	engine, ok := db.storageEngines.engines[nm.config.storageEngine]
	if !ok {
		return nil, fmt.Errorf("storage engine could not be found: %s", nm.config.storageEngine)
	}
	nm.storage, err = engine.Fork(nil)
	if err != nil {
		return nil, err
	}
	part.Map().Store(name, nm)
	return nm, nil
}

func (db *Olric) getOrCreateDMap(part *partitions.Partition, name string) (*dmap, error) {
	part.Lock()
	defer part.Unlock()
	dm, ok := part.Map().Load(name)
	if ok {
		return dm.(*dmap), nil
	}
	return db.createDMap(part, name)
}

// getDMap loads or creates a dmap.
func (db *Olric) getDMap(name string, hkey uint64) (*dmap, error) {
	part := db.primary.PartitionByHKey(hkey)
	return db.getOrCreateDMap(part, name)
}

func (db *Olric) getBackupDMap(name string, hkey uint64) (*dmap, error) {
	part := db.backup.PartitionByHKey(hkey)
	return db.getOrCreateDMap(part, name)
}
