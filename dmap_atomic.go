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

package olric

import (
	"fmt"
	"reflect"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func (db *Olric) atomicIncrDecr(opr string, w *writeop, delta int) (int, error) {
	atomicKey := w.dmap + w.key
	db.locker.Lock(atomicKey)
	defer func() {
		err := db.locker.Unlock(atomicKey)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", w.key, w.dmap, err)
		}
	}()

	entry, err := db.get(w.dmap, w.key)
	if err == ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return 0, err
	}

	var newval, curval int
	if entry != nil {
		var value interface{}
		if err := db.serializer.Unmarshal(entry.Value, &value); err != nil {
			return 0, err
		}

		// only accept integer and increase/decrease it. if the value is not integer, return an error.
		var ok bool
		curval, ok = value.(int)
		if !ok {
			return 0, fmt.Errorf("mismatched type: %v", reflect.TypeOf(value).Name())
		}
	}

	if opr == "incr" {
		newval = curval + delta
	} else if opr == "decr" {
		newval = curval - delta
	} else {
		return 0, fmt.Errorf("invalid operation")
	}

	nval, err := db.serializer.Marshal(newval)
	if err != nil {
		return 0, err
	}
	w.value = nval
	err = db.put(w)
	if err != nil {
		return 0, err
	}
	return newval, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	w := &writeop{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		timestamp:     time.Now().UnixNano(),
	}
	return dm.db.atomicIncrDecr("incr", w, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	w := &writeop{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		timestamp:     time.Now().UnixNano(),
	}
	return dm.db.atomicIncrDecr("decr", w, delta)
}

func (db *Olric) getPut(w *writeop) ([]byte, error) {
	atomicKey := w.dmap + w.key
	db.locker.Lock(atomicKey)
	defer func() {
		err := db.locker.Unlock(atomicKey)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to release the lock for key: %s on DMap: %s: %v", w.key, w.dmap, err)
		}
	}()

	entry, err := db.get(w.dmap, w.key)
	if err == ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	err = db.put(w)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return entry.Value, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (dm *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	if value == nil {
		value = struct{}{}
	}
	val, err := dm.db.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	w := &writeop{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		value:         val,
		timestamp:     time.Now().UnixNano(),
	}
	rawval, err := dm.db.getPut(w)
	if err != nil {
		return nil, err
	}

	var oldval interface{}
	if rawval != nil {
		if err := dm.db.serializer.Unmarshal(rawval, &oldval); err != nil {
			return nil, err
		}
	}
	return oldval, nil
}

func (db *Olric) exIncrDecrOperation(w, r protocol.EncodeDecoder) {
	var delta interface{}
	req := r.(*protocol.DMapMessage)
	err := db.serializer.Unmarshal(req.Value(), &delta)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	op := "incr"
	if req.Op == protocol.OpDecr {
		op = "decr"
	}
	wo := &writeop{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          req.DMap(),
		key:           req.Key(),
		timestamp:     time.Now().UnixNano(),
	}
	newval, err := db.atomicIncrDecr(op, wo, delta.(int))
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	value, err := db.serializer.Marshal(newval)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (db *Olric) exGetPutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	wo := &writeop{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          req.DMap(),
		key:           req.Key(),
		value:         req.Value(),
		timestamp:     time.Now().UnixNano(),
	}
	oldval, err := db.getPut(wo)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	if oldval != nil {
		w.SetValue(oldval)
	}
	w.SetStatus(protocol.StatusOK)
}
