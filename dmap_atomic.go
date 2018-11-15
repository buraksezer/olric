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
	"fmt"
	"reflect"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func (db *Olric) atomicIncrDecr(name, key, opr string, delta int) (int, error) {
	err := db.lockWithTimeout(name, key, time.Minute)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = db.unlock(name, key)
		if err != nil {
			db.log.Printf("[ERROR] Failed to release the lock for key: %s: %v", key, err)
		}
	}()

	rawval, err := db.get(name, key)
	if err != nil && err != ErrKeyNotFound {
		return 0, err
	}

	var newval, curval int
	if err == ErrKeyNotFound {
		err = nil
	} else {
		var value interface{}
		if err = db.serializer.Unmarshal(rawval, &value); err != nil {
			return 0, err
		}
		// switch is faster than reflect.
		switch value.(type) {
		case int:
			curval = value.(int)
		default:
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
	err = db.put(name, key, nval, nilTimeout)
	if err != nil {
		return 0, err
	}
	return newval, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	return dm.db.atomicIncrDecr(dm.name, key, "incr", delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	return dm.db.atomicIncrDecr(dm.name, key, "decr", delta)
}

func (db *Olric) getPut(name, key string, value []byte) ([]byte, error) {
	err := db.lockWithTimeout(name, key, time.Minute)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = db.unlock(name, key)
		if err != nil {
			db.log.Printf("[ERROR] Failed to release the lock for key: %s: %v", key, err)
		}
	}()

	rawval, err := db.get(name, key)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}

	err = db.put(name, key, value, nilTimeout)
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

	rawval, err := dm.db.getPut(dm.name, key, val)
	if err != nil {
		return nil, err
	}

	var oldval interface{}
	if rawval != nil {
		if err = dm.db.serializer.Unmarshal(rawval, &oldval); err != nil {
			return nil, err
		}
	}
	return oldval, nil
}

func (db *Olric) exIncrDecrOperation(req *protocol.Message) *protocol.Message {
	var delta interface{}
	err := db.serializer.Unmarshal(req.Value, &delta)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	op := "incr"
	if req.Op == protocol.OpExDecr {
		op = "decr"
	}
	newval, err := db.atomicIncrDecr(req.DMap, req.Key, op, delta.(int))
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	data, err := db.serializer.Marshal(newval)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	resp := req.Success()
	resp.Value = data
	return resp
}

func (db *Olric) exGetPutOperation(req *protocol.Message) *protocol.Message {
	oldval, err := db.getPut(req.DMap, req.Key, req.Value)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	resp := req.Success()
	if oldval != nil {
		resp.Value = oldval
	}
	return resp
}
