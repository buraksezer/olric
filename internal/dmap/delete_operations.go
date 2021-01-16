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
	"github.com/buraksezer/olric/pkg/storage"
)

func (s *Service) exDeleteOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		// TODO: Consider returning ErrKeyNotFound.
		w.SetStatus(protocol.StatusOK)
		return
	}
	if err != nil {
		errorResponse(w, err)
		return
	}

	err = dm.deleteKey(req.Key())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) deletePrevOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		// TODO: Consider returning ErrKeyNotFound.
		w.SetStatus(protocol.StatusOK)
		return
	}
	if err != nil {
		errorResponse(w, err)
		return
	}

	err = dm.deleteOnPreviousOwner(req.Key())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (s *Service) deleteBackupOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := partitions.HKey(req.DMap(), req.Key())

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

	f, err := dm.getFragment(hkey, partitions.BACKUP)
	if err != nil {
		errorResponse(w, err)
		return
	}
	f.Lock()
	defer f.Unlock()

	err = f.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		dm.s.wg.Add(1)
		go dm.s.callCompactionOnStorage(f)
		err = nil
	}
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
