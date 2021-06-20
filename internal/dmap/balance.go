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
	"fmt"
	"github.com/buraksezer/olric/pkg/neterrors"

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
	if errors.Is(err, storage.ErrKeyNotFound) {
		return entry, nil
	}
	if err != nil {
		return nil, err
	}
	versions := []*version{{entry: current}, {entry: entry}}
	versions = dm.sortVersions(versions)
	return versions[0].entry, nil
}

func (dm *DMap) mergeFragments(part *partitions.Partition, fp *fragmentPack) error {
	f, err := dm.loadOrCreateFragment(part)
	if err != nil {
		return err
	}

	// Acquire fragment's lock. No one should work on it.
	f.Lock()
	defer f.Unlock()

	// TODO: This may be useless. Check it.
	defer part.Map().Store(fp.Name, f)

	s, err := f.storage.Import(fp.Payload)
	if err != nil {
		return err
	}

	// Merge accessLog.
	if dm.config != nil && dm.config.isAccessLogRequired() {
		f.accessLog = &accessLog{
			m: fp.AccessLog,
		}
	}

	if f.storage.Stats().Length == 0 {
		// DMap has no keys. Set the imported storage instance.
		// The old one will be garbage collected.
		f.storage = s
		return nil
	}

	// DMap has some keys. Merge with the new one.
	var mergeErr error
	s.Range(func(hkey uint64, entry storage.Entry) bool {
		winner, err := dm.selectVersionForMerge(f, hkey, entry)
		if err != nil {
			mergeErr = err
			return false
		}
		// TODO: Don't put the winner again if it comes from dm.storage
		mergeErr = f.storage.Put(hkey, winner)
		if errors.Is(mergeErr, storage.ErrFragmented) {
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

func (s *Service) validateFragmentPack(fp *fragmentPack) error {
	if fp.PartID >= s.config.PartitionCount {
		return fmt.Errorf("invalid partition id: %d", fp.PartID)
	}

	var part *partitions.Partition
	if fp.Kind == partitions.PRIMARY {
		part = s.primary.PartitionByID(fp.PartID)
	} else {
		part = s.backup.PartitionByID(fp.PartID)
	}

	// Check ownership before merging. This is useful to prevent data corruption in network partitioning case.
	if !s.checkOwnership(part) {
		return neterrors.Wrap(neterrors.ErrInvalidArgument,
			fmt.Sprintf("partID: %d (kind: %s) doesn't belong to %s", fp.PartID, fp.Kind, s.rt.This()))
	}
	return nil
}

func (s *Service) extractFragmentPack(r protocol.EncodeDecoder) (*fragmentPack, error) {
	req := r.(*protocol.SystemMessage)
	fp := &fragmentPack{}
	err := msgpack.Unmarshal(req.Value(), fp)
	return fp, err
}

func (s *Service) moveFragmentOperation(w, r protocol.EncodeDecoder) {
	fp, err := s.extractFragmentPack(r)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to unmarshal DMap: %v", err)
		neterrors.ErrorResponse(w, err)
		return
	}

	if err = s.validateFragmentPack(fp); err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	var part *partitions.Partition
	if fp.Kind == partitions.PRIMARY {
		part = s.primary.PartitionByID(fp.PartID)
	} else {
		part = s.backup.PartitionByID(fp.PartID)
	}
	s.log.V(2).Printf("[INFO] Received DMap (kind: %s): %s on PartID: %d", fp.Kind, fp.Name, fp.PartID)

	dm, err := s.NewDMap(fp.Name)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	err = dm.mergeFragments(part, fp)
	if err != nil {
		s.log.V(2).Printf("[ERROR] Failed to merge Received DMap (kind: %s): %s on PartID: %d: %v",
			fp.Kind, fp.Name, fp.PartID, err)
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
