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
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

func (s *Service) exPutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		dm, err = s.NewDMap(req.DMap())
		if err != nil {
			errorResponse(w, err)
			return
		}
	}

	if err != nil {
		errorResponse(w, err)
		return
	}

	wo := &writeop{}
	wo.fromReq(r, partitions.PRIMARY)
	err = dm.put(wo)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) putReplicaOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		dm, err = s.NewDMap(req.DMap())
		if err != nil {
			errorResponse(w, err)
			return
		}
	}

	if err != nil {
		errorResponse(w, err)
		return
	}

	hkey := partitions.HKey(req.DMap(), req.Key())
	wo := &writeop{}
	wo.fromReq(req, partitions.BACKUP)
	err = dm.localPut(hkey, wo)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
