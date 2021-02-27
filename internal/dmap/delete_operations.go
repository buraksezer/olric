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


func (s *Service) deleteOperationCommon(w, r protocol.EncodeDecoder, f func(dm *DMap, w, r protocol.EncodeDecoder) error) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getDMap(req.DMap())
	if err == ErrDMapNotFound {
		// we don't even have the DMap. It's just OK.
		w.SetStatus(protocol.StatusOK)
		return
	}
	if err != nil {
		errorResponse(w, err)
		return
	}
	err = f(dm, w, r)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) deleteOperation(w, r protocol.EncodeDecoder) {
	s.deleteOperationCommon(w, r, func(dm *DMap, w, r protocol.EncodeDecoder) error {
		req := r.(*protocol.DMapMessage)
		return dm.deleteKey(req.Key())
	})
}

func (s *Service) deletePrevOperation(w, r protocol.EncodeDecoder) {
	s.deleteOperationCommon(w, r, func(dm *DMap, w, r protocol.EncodeDecoder) error {
		req := r.(*protocol.DMapMessage)
		return dm.deleteBackupFromFragment(req.Key(), partitions.PRIMARY)
	})
}

func (s *Service) deleteBackupOperation(w, r protocol.EncodeDecoder) {
	s.deleteOperationCommon(w, r, func(dm *DMap, w, r protocol.EncodeDecoder) error {
		req := r.(*protocol.DMapMessage)
		return dm.deleteBackupFromFragment(req.Key(), partitions.BACKUP)
	})
}
