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
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) putOperationCommon(w, r protocol.EncodeDecoder, f func(dm *DMap, r protocol.EncodeDecoder) error) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	err = f(dm, r)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) putOperation(w, r protocol.EncodeDecoder) {
	s.putOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) error {
		e := newEnvFromReq(r, partitions.PRIMARY)
		return dm.put(e)
	})
}

func (s *Service) putReplicaOperation(w, r protocol.EncodeDecoder) {
	s.putOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) error {
		e := newEnvFromReq(r, partitions.BACKUP)
		return dm.putOnReplicaFragment(e)
	})
}
