// Copyright 2018 Burak Sezer
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

package olric

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
)

func (db *Olric) unmarshalValue(rawval []byte) (interface{}, error) {
	var value interface{}
	err := db.serializer.Unmarshal(rawval, &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

func (db *Olric) getKeyVal(hkey uint64, name, key string) ([]byte, error) {
	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return nil, err
	}
	value, err := dm.str.Get(hkey)
	if err == nil {
		if isKeyExpired(value.TTL) {
			return nil, ErrKeyNotFound
		}
		return value.Value, nil
	}

	// Run a query on the previous owners.
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}
	owners = owners[:len(owners)-1]
	for i := 1; i <= len(owners); i++ {
		// Traverse in reverse order.
		idx := len(owners) - i
		owner := owners[idx]
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		resp, err := db.requestTo(owner.String(), protocol.OpGetPrev, req)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		return resp.Value, err
	}

	// Check backups.
	backups := db.getBackupPartitionOwners(hkey)
	for _, backup := range backups {
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		resp, err := db.requestTo(backup.String(), protocol.OpGetBackup, req)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		return resp.Value, nil
	}

	// It's not there, really.
	return nil, ErrKeyNotFound
}

func (db *Olric) get(name, key string) ([]byte, error) {
	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return nil, err
	}
	if !hostCmp(member, db.this) {
		req := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		resp, err := db.requestTo(member.String(), protocol.OpExGet, req)
		if err != nil {
			return nil, err
		}
		return resp.Value, nil
	}

	return db.getKeyVal(hkey, name, key)
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.
// It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.
func (dm *DMap) Get(key string) (interface{}, error) {
	rawval, err := dm.db.get(dm.name, key)
	if err != nil {
		return nil, err
	}
	return dm.db.unmarshalValue(rawval)
}

func (db *Olric) exGetOperation(req *protocol.Message) *protocol.Message {
	value, err := db.get(req.DMap, req.Key)
	if err == ErrKeyNotFound {
		return req.Error(protocol.StatusKeyNotFound, "")
	}
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	resp := req.Success()
	resp.Value = value
	return resp
}

func (db *Olric) getBackupOperation(req *protocol.Message) *protocol.Message {
	// TODO: We may need to check backup ownership
	hkey := db.getHKey(req.DMap, req.Key)
	dm, err := db.getBackupDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	vdata, err := dm.str.Get(hkey)
	if err == storage.ErrKeyNotFound {
		return req.Error(protocol.StatusKeyNotFound, "")
	}
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	if isKeyExpired(vdata.TTL) {
		return req.Error(protocol.StatusKeyNotFound, "")
	}

	resp := req.Success()
	resp.Value = vdata.Value
	return resp
}

func (db *Olric) getPrevOperation(req *protocol.Message) *protocol.Message {
	hkey := db.getHKey(req.DMap, req.Key)
	part := db.getPartition(hkey)
	tmp, ok := part.m.Load(req.DMap)
	if !ok {
		return req.Error(protocol.StatusKeyNotFound, "")
	}
	dm := tmp.(*dmap)

	vdata, err := dm.str.Get(hkey)
	if err == storage.ErrKeyNotFound {
		return req.Error(protocol.StatusKeyNotFound, "")
	}
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	if isKeyExpired(vdata.TTL) {
		return req.Error(protocol.StatusKeyNotFound, "")
	}
	resp := req.Success()
	resp.Value = vdata.Value
	return resp
}
