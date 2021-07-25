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
	"errors"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

func (dm *DMap) loadCurrentAtomicInt(e *env) (int, error) {
	entry, err := dm.get(e.key)
	if errors.Is(err, ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return 0, err
	}

	var current int
	if entry != nil {
		var value interface{}
		if err := dm.s.serializer.Unmarshal(entry.Value(), &value); err != nil {
			return 0, err
		}
		return valueToInt(value)
	}
	return current, nil
}

func (dm *DMap) atomicIncrDecr(opcode protocol.OpCode, e *env, delta int) (int, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the fine grained lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	current, err := dm.loadCurrentAtomicInt(e)
	if err != nil {
		return 0, err
	}

	var updated int
	switch {
	case opcode == protocol.OpIncr:
		updated = current + delta
	case opcode == protocol.OpDecr:
		updated = current - delta
	default:
		return 0, fmt.Errorf("invalid operation")
	}

	val, err := dm.s.serializer.Marshal(updated)
	if err != nil {
		return 0, err
	}

	e.value = val
	err = dm.put(e)
	if err != nil {
		return 0, err
	}

	return updated, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	e := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		timestamp:     time.Now().UnixNano(),
		kind:          partitions.PRIMARY,
	}
	return dm.atomicIncrDecr(protocol.OpIncr, e, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	e := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		timestamp:     time.Now().UnixNano(),
		kind:          partitions.PRIMARY,
	}
	return dm.atomicIncrDecr(protocol.OpDecr, e, delta)
}

func (dm *DMap) getPut(e *env) ([]byte, error) {
	atomicKey := e.dmap + e.key
	dm.s.locker.Lock(atomicKey)
	defer func() {
		err := dm.s.locker.Unlock(atomicKey)
		if err != nil {
			dm.s.log.V(3).Printf("[ERROR] Failed to release the lock for key: %s on DMap: %s: %v", e.key, e.dmap, err)
		}
	}()

	entry, err := dm.get(e.key)
	if errors.Is(err, ErrKeyNotFound) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	err = dm.put(e)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		// The value is nil.
		return nil, nil
	}
	return entry.Value(), nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (dm *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	if value == nil {
		value = struct{}{}
	}
	val, err := dm.s.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	e := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          dm.name,
		key:           key,
		value:         val,
		timestamp:     time.Now().UnixNano(),
	}
	raw, err := dm.getPut(e)
	if err != nil {
		return nil, err
	}

	var old interface{}
	if raw != nil {
		if err = dm.s.serializer.Unmarshal(raw, &old); err != nil {
			return nil, err
		}
	}
	return old, nil
}
