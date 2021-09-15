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

package client

import (
	"fmt"
	"reflect"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
)

// DMap provides methods to access distributed maps on Olric cluster.
type DMap struct {
	*Client
	entryFormat storage.Entry
	name        string
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key.
// It's thread-safe. It is safe to modify the contents of the returned value.
func (d *DMap) Get(key string) (interface{}, error) {
	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetDMap(d.name)
	req.SetKey(key)
	resp, err := d.request(req)
	if err != nil {
		return nil, err
	}
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}
	entry := d.getEntryFormat(d.name)
	entry.Decode(resp.Value())
	return d.unmarshalValue(entry.Value())
}

// GetEntry gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key.
// It's thread-safe. It is safe to modify the contents of the returned value.
func (d *DMap) GetEntry(key string) (*olric.Entry, error) {
	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetDMap(d.name)
	req.SetKey(key)
	resp, err := d.request(req)
	if err != nil {
		return nil, err
	}
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}

	entry := d.getEntryFormat(d.name)
	entry.Decode(resp.Value())
	value, err := d.unmarshalValue(entry.Value())
	if err != nil {
		return nil, err
	}

	return &olric.Entry{
		Key:       entry.Key(),
		TTL:       entry.TTL(),
		Timestamp: entry.Timestamp(),
		Value:     value,
	}, nil
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) Put(key string, value interface{}) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(protocol.OpPut)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutExtra{
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key.
// It's thread-safe. It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(protocol.OpPutEx)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutExExtra{
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist.
// It's thread-safe. It is safe to modify the contents of the argument after Delete returns.
func (d *DMap) Delete(key string) error {
	req := protocol.NewDMapMessage(protocol.OpDelete)
	req.SetDMap(d.name)
	req.SetKey(key)
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// LockContext is returned by Lock and LockWithTimeout methods.
// It should be stored in a proper way to release the lock.
type LockContext struct {
	name  string
	key   string
	token []byte
	dmap  *DMap
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (d *DMap) LockWithTimeout(key string, timeout, deadline time.Duration) (*LockContext, error) {
	req := protocol.NewDMapMessage(protocol.OpLockWithTimeout)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetExtra(protocol.LockWithTimeoutExtra{
		Timeout:  timeout.Nanoseconds(),
		Deadline: deadline.Nanoseconds(),
	})
	resp, err := d.request(req)
	if err != nil {
		return nil, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return nil, err
	}
	ctx := &LockContext{
		name:  d.name,
		key:   key,
		token: resp.Value(),
		dmap:  d,
	}
	return ctx, nil
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (d *DMap) Lock(key string, deadline time.Duration) (*LockContext, error) {
	req := protocol.NewDMapMessage(protocol.OpLock)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetExtra(protocol.LockExtra{
		Deadline: deadline.Nanoseconds(),
	})
	resp, err := d.request(req)
	if err != nil {
		return nil, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return nil, err
	}
	ctx := &LockContext{
		name:  d.name,
		key:   key,
		token: resp.Value(),
		dmap:  d,
	}
	return ctx, nil
}

// Unlock releases an acquired lock for the given key.
// It returns olric.ErrNoSuchLock if there is no lock for the given key.
func (l *LockContext) Unlock() error {
	req := protocol.NewDMapMessage(protocol.OpUnlock)
	req.SetDMap(l.name)
	req.SetKey(l.key)
	req.SetValue(l.token)
	resp, err := l.dmap.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// Lease update the expiry of an acquired lock for the given key.
// It returns olric.ErrNoSuchLock if there is no lock or already expired for the given key.
func (l *LockContext) Lease(timeout time.Duration) error {
	req := protocol.NewDMapMessage(protocol.OpLockLease)
	req.SetDMap(l.name)
	req.SetKey(l.key)
	req.SetValue(l.token)
	req.SetExtra(protocol.LockLeaseExtra{
		Timeout: int64(timeout),
	})
	resp, err := l.dmap.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// Destroy flushes the given dmap on the cluster. You should know that there is no global lock on DMaps.
// So if you call Put/PutEx/PutIf/PutIfEx and Destroy methods concurrently on the cluster,
// those calls may set new values to the dmap.
func (d *DMap) Destroy() error {
	req := protocol.NewDMapMessage(protocol.OpDestroy)
	req.SetDMap(d.name)
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func valueToInt(delta interface{}) (int, error) {
	switch value := delta.(type) {
	case int:
		return value, nil
	case int8:
		return int(value), nil
	case int16:
		return int(value), nil
	case int32:
		return int(value), nil
	case int64:
		return int(value), nil
	default:
		return 0, fmt.Errorf("mismatched type: %v", reflect.TypeOf(delta))
	}
}

func (c *Client) processIncrDecrResponse(resp protocol.EncodeDecoder) (int, error) {
	if err := checkStatusCode(resp); err != nil {
		return 0, err
	}
	value, err := c.unmarshalValue(resp.Value())
	if err != nil {
		return 0, err
	}
	return valueToInt(value)
}

func (c *Client) incrDecr(op protocol.OpCode, name, key string, delta int) (int, error) {
	value, err := c.serializer.Marshal(delta)
	if err != nil {
		fmt.Println(delta, err)
		return 0, err
	}
	req := protocol.NewDMapMessage(op)
	req.SetDMap(name)
	req.SetKey(key)
	req.SetValue(value)
	req.SetExtra(protocol.AtomicExtra{
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := c.request(req)
	if err != nil {
		return 0, err
	}
	return c.processIncrDecrResponse(resp)
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (d *DMap) Incr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpIncr, d.name, key, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (d *DMap) Decr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpDecr, d.name, key, delta)
}

func (c *Client) processGetPutResponse(resp protocol.EncodeDecoder) (interface{}, error) {
	if err := checkStatusCode(resp); err != nil {
		return nil, err
	}
	if len(resp.Value()) == 0 {
		return nil, nil
	}
	old, err := c.unmarshalValue(resp.Value())
	if err != nil {
		return nil, err
	}
	return old, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (d *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	req := protocol.NewDMapMessage(protocol.OpGetPut)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.AtomicExtra{
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return nil, err
	}
	return d.processGetPutResponse(resp)
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (d *DMap) Expire(key string, timeout time.Duration) error {
	req := protocol.NewDMapMessage(protocol.OpExpire)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetExtra(protocol.ExpireExtra{
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutIf sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after PutIf returns but not before.
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (d *DMap) PutIf(key string, value interface{}, flags int16) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(protocol.OpPutIf)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutIfExtra{
		Flags:     flags,
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

// PutIfEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// It is safe to modify the contents of the arguments after PutIfEx returns but not before.
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (d *DMap) PutIfEx(key string, value interface{}, timeout time.Duration, flags int16) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(protocol.OpPutIfEx)
	req.SetDMap(d.name)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutIfExExtra{
		Flags:     flags,
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	resp, err := d.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}
