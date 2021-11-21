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

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) destroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	err = dm.destroyOnCluster()
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (dm *DMap) destroyFragmentOnPartition(part *partitions.Partition) error {
	f, err := dm.loadFragment(part)
	if errors.Is(err, errFragmentNotFound) {
		// not exists
		return nil
	}
	if err != nil {
		return err
	}
	return wipeOutFragment(part, dm.fragmentName, f)
}

func (s *Service) destroyDMapOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	// This is very similar with rm -rf. Destroys given dmap on the cluster
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		dm, err := s.getDMap(req.DMap())
		if errors.Is(err, ErrDMapNotFound) {
			continue
		}
		if err != nil {
			neterrors.ErrorResponse(w, err)
			return
		}

		part := dm.s.primary.PartitionByID(partID)
		err = dm.destroyFragmentOnPartition(part)
		if err != nil {
			neterrors.ErrorResponse(w, err)
			return
		}

		// Destroy on replicas
		if s.config.ReplicaCount > config.MinimumReplicaCount {
			backup := dm.s.backup.PartitionByID(partID)
			err = dm.destroyFragmentOnPartition(backup)
			if err != nil {
				neterrors.ErrorResponse(w, err)
				return
			}
		}
	}

	s.Lock()
	delete(s.dmaps, req.DMap())
	s.Unlock()
	w.SetStatus(protocol.StatusOK)
}
