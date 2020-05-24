// Copyright 2018-2019 Burak Sezer
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
	"github.com/buraksezer/olric/internal/protocol"
	"reflect"
)

func (db *Olric) atomicIncrDecr(opcode protocol.OpCode, w *writeop, delta int) (int, error) {
	atomicKey := w.dmap + w.key
	db.locker.Lock(atomicKey)
	defer func() {
		err := db.locker.Unlock(atomicKey)
		if err != nil {
			db.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", w.key, w.dmap, err)
		}
	}()

	rawval, err := db.get(w.dmap, w.key)
	if err == ErrKeyNotFound {
		err = nil
	}
	if err != nil {
		return 0, err
	}

	var newval, curval int
	if len(rawval) != 0 {
		var value interface{}
		if err := db.serializer.Unmarshal(rawval, &value); err != nil {
			return 0, err
		}

		// only accept integer and increase/decrease it. if the value is not integer, return an error.
		var ok bool
		curval, ok = value.(int)
		if !ok {
			return 0, fmt.Errorf("mismatched type: %v", reflect.TypeOf(value).Name())
		}
	}

	if opcode == protocol.OpIncr {
		newval = curval + delta
	} else if opcode == protocol.OpDecr {
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
	w, err := dm.db.prepareWriteop(protocol.OpPut, dm.name, key, nil, nilTimeout, 0, false)
	if err != nil {
		return 0, err
	}
	return dm.db.atomicIncrDecr(protocol.OpIncr, w, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	w, err := dm.db.prepareWriteop(protocol.OpPut, dm.name, key, nil, nilTimeout, 0, false)
	if err != nil {
		return 0, err
	}
	return dm.db.atomicIncrDecr(protocol.OpDecr, w, delta)
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

	rawval, err := db.get(w.dmap, w.key)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}
	err = db.put(w)
	if err != nil {
		return nil, err
	}
	return rawval, nil
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
	w, err := dm.db.prepareWriteop(protocol.OpPut, dm.name, key, val, nilTimeout, 0, true)
	if err != nil {
		return nil, err
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

func (db *Olric) exIncrDecrOperation(req *protocol.Message) *protocol.Message {
	var delta interface{}
	err := db.serializer.Unmarshal(req.Value, &delta)
	if err != nil {
		return db.prepareResponse(req, err)
	}

	w, err := db.prepareWriteop(protocol.OpPut, req.DMap, req.Key, nil, nilTimeout, 0, false)
	if err != nil {
		return db.prepareResponse(req, err)
	}

	newval, err := db.atomicIncrDecr(req.Op, w, delta.(int))
	if err != nil {
		return db.prepareResponse(req, err)
	}

	data, err := db.serializer.Marshal(newval)
	if err != nil {
		return db.prepareResponse(req, err)
	}
	resp := req.Success()
	resp.Value = data
	return resp
}

func (db *Olric) exGetPutOperation(req *protocol.Message) *protocol.Message {
	w, err := db.prepareWriteop(protocol.OpPut, req.DMap, req.Key, req.Value, nilTimeout, 0, true)
	if err != nil {
		return db.prepareResponse(req, err)
	}
	oldval, err := db.getPut(w)
	if err != nil {
		return db.prepareResponse(req, err)
	}
	resp := req.Success()
	if oldval != nil {
		resp.Value = oldval
	}
	return resp
}
