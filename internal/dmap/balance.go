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

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/vmihailenco/msgpack"
)

type fragmentPack struct {
	PartID    uint64
	Kind      partitions.Kind
	Name      string
	Payload   []byte
	AccessLog map[uint64]int64
}

func (dm *DMap) selectVersionForMerge(f *fragment, hkey uint64, entry storage.Entry) (storage.Entry, error) {
	current, err := f.storage.Get(hkey)
	if err == storage.ErrKeyNotFound {
		return entry, nil
	}
	if err != nil {
		return nil, err
	}
	versions := []*version{{entry: current}, {entry: entry}}
	versions = dm.sortVersions(versions)
	return versions[0].entry, nil
}

func (dm *DMap) mergeFragments(part *partitions.Partition, data *fragmentPack) error {
	f, err := dm.loadFragmentFromPartition(part, data.Name)
	if err != nil {
		return err
	}

	// Acquire dmap's lock. No one should work on it.
	f.Lock()
	defer f.Unlock()
	defer part.Map().Store(data.Name, f)

	engine, err := f.storage.Import(data.Payload)
	if err != nil {
		return err
	}

	// Merge accessLog.
	if dm.config != nil && dm.config.accessLog != nil {
		dm.config.Lock()
		for hkey, t := range data.AccessLog {
			if _, ok := dm.config.accessLog[hkey]; !ok {
				dm.config.accessLog[hkey] = t
			}
		}
		dm.config.Unlock()
	}

	if f.storage.Stats().Length == 0 {
		// DMap has no keys. Set the imported storage instance.
		// The old one will be garbage collected.
		f.storage = engine
		return nil
	}

	// DMap has some keys. Merge with the new one.
	var mergeErr error
	engine.Range(func(hkey uint64, entry storage.Entry) bool {
		winner, err := dm.selectVersionForMerge(f, hkey, entry)
		if err != nil {
			mergeErr = err
			return false
		}
		// TODO: Don't put the winner again if it comes from dm.storage
		mergeErr = f.storage.Put(hkey, winner)
		if mergeErr == storage.ErrFragmented {
			dm.s.wg.Add(1)
			go dm.s.callCompactionOnStorage(f)
			mergeErr = nil
		}
		if mergeErr != nil {
			return false
		}
		return true
	})
	return mergeErr
}

func (s *Service) checkOwnership(part *partitions.Partition) bool {
	owners := part.Owners()
	for _, owner := range owners {
		if owner.CompareByID(s.rt.This()) {
			return true
		}
	}
	return false
}

func (s *Service) moveDMapOperation(w, r protocol.EncodeDecoder) {
	if err := s.rt.CheckMemberCountQuorum(); err != nil {
		errorResponse(w, err)
		return
	}
	// An Olric node has to be bootstrapped to function properly.
	if err := s.rt.CheckBootstrap(); err != nil {
		errorResponse(w, err)
		return
	}

	req := r.(*protocol.SystemMessage)
	box := &fragmentPack{}
	err := msgpack.Unmarshal(req.Value(), box)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to unmarshal dmap: %v", err)
		errorResponse(w, err)
		return
	}

	var part *partitions.Partition
	if box.Kind == partitions.PRIMARY {
		part = s.primary.PartitionById(box.PartID)
	} else {
		part = s.backup.PartitionById(box.PartID)
	}

	// Check ownership before merging. This is useful to prevent data corruption in network partitioning case.
	if !s.checkOwnership(part) {
		s.log.V(2).Printf("[ERROR] Received DMap: %s on PartID: %d (kind: %s) doesn't belong to this node (%s)",
			box.Name, box.PartID, box.Kind, s.rt.This())
		err := fmt.Errorf("partID: %d (kind: %s) doesn't belong to %s: %w", box.PartID, box.Kind, s.rt.This(), ErrInvalidArgument)
		errorResponse(w, err)
		return
	}

	s.log.V(2).Printf("[INFO] Received DMap (kind: %s): %s on PartID: %d", box.Kind, box.Name, box.PartID)

	dm, err := s.NewDMap(box.Name)
	if err != nil {
		errorResponse(w, err)
		return
	}

	err = dm.mergeFragments(part, box)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to merge DMap: %v", err)
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
