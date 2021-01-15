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

func (s *Service) exGetOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
	}

	entry, err := dm.get(req.Key())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}

func (s *Service) getPrevOrBackupCommon(w, r protocol.EncodeDecoder, kind partitions.Kind) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
	}

	e := newEnvFromReq(r, kind)
	entry, err := dm.getOnFragment(e)
	if err != nil {
		errorResponse(w, err)
	}

	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}

func (s *Service) getBackupOperation(w, r protocol.EncodeDecoder) {
	s.getPrevOrBackupCommon(w, r, partitions.BACKUP)
}

func (s *Service) getPrevOperation(w, r protocol.EncodeDecoder) {
	s.getPrevOrBackupCommon(w, r, partitions.PRIMARY)

}
