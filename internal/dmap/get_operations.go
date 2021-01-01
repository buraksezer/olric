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

func (db *Olric) exGetOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	entry, err := db.get(req.DMap(), req.Key())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}

func (db *Olric) getBackupOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := partitions.HKey(req.DMap(), req.Key())
	dm, err := db.getBackupDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.RLock()
	defer dm.RUnlock()
	entry, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	if isKeyExpired(entry.TTL()) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}

func (db *Olric) getPrevOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := partitions.HKey(req.DMap(), req.Key())
	part := db.primary.PartitionByHKey(hkey)
	tmp, ok := part.Map().Load(req.DMap())
	if !ok {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	dm := tmp.(*dmap)

	entry, err := dm.storage.Get(hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	if isKeyExpired(entry.TTL()) {
		db.errorResponse(w, ErrKeyNotFound)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(entry.Encode())
}
