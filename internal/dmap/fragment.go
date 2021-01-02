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
	"sync"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/pkg/storage"
)

type fragment struct {
	sync.RWMutex

	service *Service
	storage storage.Engine
}

func (f *fragment) Name() string {
	return "DMap"
}

func (f *fragment) Length() int {
	f.RLock()
	defer f.RUnlock()

	return f.storage.Stats().Length
}

func (f *fragment) Move(_ uint64, _ partitions.Kind, _ string, _ discovery.Member) error {
	return nil
}

func (dm *DMap) loadFragmentFromPartition(part *partitions.Partition, name string) (*fragment, error) {
	f, ok := part.Map().Load(name)
	if !ok {
		return nil, errFragmentNotFound
	}
	return f.(*fragment), nil
}

func (dm *DMap) createFragmentOnPartition(part *partitions.Partition, name string) (*fragment, error) {
	engine, ok := dm.service.storage.engines[dm.config.storageEngine]
	if !ok {
		return nil, fmt.Errorf("storage engine could not be found: %s", dm.config.storageEngine)
	}
	f := &fragment{}
	var err error
	f.storage, err = engine.Fork(nil)
	if err != nil {
		return nil, err
	}
	part.Map().Store(name, f)
	return f, nil
}

func (dm *DMap) getPartitionByHKey(hkey uint64, kind partitions.Kind) *partitions.Partition {
	var part *partitions.Partition
	if kind == partitions.PRIMARY {
		part = dm.service.primary.PartitionByHKey(hkey)
	} else if kind == partitions.BACKUP {
		part = dm.service.backup.PartitionByHKey(hkey)
	} else {
		// impossible
		panic("unknown partition kind")
	}
	return part
}

func (dm *DMap) getFragment(name string, hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()
	return dm.loadFragmentFromPartition(part, name)
}

func (dm *DMap) getOrCreateFragment(name string, hkey uint64, kind partitions.Kind) (*fragment, error) {
	part := dm.getPartitionByHKey(hkey, kind)
	part.Lock()
	defer part.Unlock()

	// try to get
	f, err := dm.loadFragmentFromPartition(part, name)
	if err == errFragmentNotFound {
		// create the fragment and return
		return dm.createFragmentOnPartition(part, name)
	}
	return f, err
}

var _ partitions.Fragment = (*fragment)(nil)
