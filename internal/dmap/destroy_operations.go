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

import "github.com/buraksezer/olric/internal/protocol"

func (s *Service) exDestroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
	}
	err = dm.destroy(req.DMap())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) destroyDMapOperation(w, r protocol.EncodeDecoder) {
	// 1- Destroy fragments on primary
	// 2- Destroy fragments on backup
	// 3- Remove DMap from the service cache
	// 4- Be sure all background threads are stopped.
	req := r.(*protocol.DMapMessage)
	// This is very similar with rm -rf. Destroys given dmap on the cluster
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		dm, err := s.LoadDMap(req.DMap())
		if err == ErrDMapNotFound {
			continue
		}
		if err != nil {
			// TODO: Log this
			return
		}

		part := s.primary.PartitionById(partID)
		f, err := dm.loadFragmentFromPartition(part, req.DMap())
		if err != nil {
			// TODO: Log this
			return
		}
		f.Destroy()
		// Delete primary copies
		part.Map().Delete(req.DMap())
		// Delete from Backups
		if s.config.ReplicaCount != 0 {
			bpart := s.backup.PartitionById(partID)
			bpart.Map().Delete(req.DMap())
		}
	}
	w.SetStatus(protocol.StatusOK)
}
