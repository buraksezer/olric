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

	"github.com/buraksezer/olric/internal/encoding"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/util"
	"github.com/buraksezer/olric/pkg/storage"
)

func (dm *DMap) loadCurrentAtomicInt(e *env) (int, error) {
	entry, err := dm.get(e.key)
	if errors.Is(err, ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	if entry == nil {
		return 0, nil
	}
	nr, err := util.ParseInt(entry.Value(), 10, 64)
	if err != nil {
		return 0, nil
	}
	return int(nr), nil
}

func (dm *DMap) atomicIncrDecr(cmd string, e *env, delta int) (int, error) {
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
	switch cmd {
	case resp.IncrCmd:
		updated = current + delta
	case resp.DecrCmd:
		updated = current - delta
	default:
		return 0, fmt.Errorf("invalid operation")
	}

	valueBuf := pool.Get()
	enc := encoding.New(valueBuf)
	err = enc.Encode(updated)
	if err != nil {
		return 0, err
	}
	e.value = valueBuf.Bytes()
	defer func() {
		pool.Put(valueBuf)
	}()

	err = dm.put(e)
	if err != nil {
		return 0, err
	}

	return updated, nil
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	e := newEnv()
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(resp.IncrCmd, e, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	e := newEnv()
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(resp.DecrCmd, e, delta)
}

func (dm *DMap) getPut(e *env) (storage.Entry, error) {
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
	return entry, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (dm *DMap) GetPut(key string, value interface{}) (*GetResponse, error) {
	if value == nil {
		value = struct{}{}
	}

	valueBuf := pool.Get()
	enc := encoding.New(valueBuf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	defer func() {
		pool.Put(valueBuf)
	}()

	e := newEnv()
	e.dmap = dm.name
	e.key = key
	e.value = valueBuf.Bytes()
	raw, err := dm.getPut(e)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	return &GetResponse{entry: raw}, nil
}
