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

/*Package client implements a Golang client to access an Olric cluster from outside. */
package client

import (
	"fmt"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
)

// Client implements Go client of Olric Binary Protocol and its methods.
type Client struct {
	client     *transport.Client
	serializer olric.Serializer
}

// Config includes configuration parameters for the Client.
type Config struct {
	Addrs       []string
	DialTimeout time.Duration
	KeepAlive   time.Duration
	MaxConn     int
}

// DMap provides methods to access distributed maps on Olric cluster.
type DMap struct {
	*Client
	name string
}

// New returns a new Client object. The second parameter is serializer, it can be nil.
func New(c *Config, s olric.Serializer) (*Client, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if len(c.Addrs) == 0 {
		return nil, fmt.Errorf("addrs list cannot be empty")
	}
	if s == nil {
		s = olric.NewGobSerializer()
	}
	if c.MaxConn == 0 {
		c.MaxConn = 1
	}
	cc := &transport.ClientConfig{
		Addrs:       c.Addrs,
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlive,
		MaxConn:     c.MaxConn,
	}
	return &Client{
		client:     transport.NewClient(cc),
		serializer: s,
	}, nil
}

// Close cancels underlying context and cancels ongoing requests.
func (c *Client) Close() {
	c.client.Close()
}

// NewDMap creates and returns a new DMap object to access DMaps on the cluster.
func (c *Client) NewDMap(name string) *DMap {
	return &DMap{
		Client: c,
		name:   name,
	}
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.
// It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.
func (d *DMap) Get(key string) (interface{}, error) {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
	}
	resp, err := d.client.Request(protocol.OpExGet, m)
	if err != nil {
		return nil, err
	}
	if resp.Status == protocol.StatusKeyNotFound {
		return nil, olric.ErrKeyNotFound
	}
	var value interface{}
	err = d.serializer.Unmarshal(resp.Value, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) Put(key string, value interface{}) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
	}
	_, err = d.client.Request(protocol.OpExPut, m)
	return err
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// It is safe to modify the contents of the arguments after Put returns but not before.
func (d *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Extra: protocol.PutExExtra{TTL: timeout.Nanoseconds()},
		Value: data,
	}
	_, err = d.client.Request(protocol.OpExPutEx, m)
	return err
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (d *DMap) Delete(key string) error {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
	}
	_, err := d.client.Request(protocol.OpExDelete, m)
	return err
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this map. Please note that, before
// setting a lock for a key, you should set the key with Put method. Otherwise it returns olric.ErrKeyNotFound error.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until timeout.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (d *DMap) LockWithTimeout(key string, timeout time.Duration) error {
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Extra: protocol.LockWithTimeoutExtra{TTL: timeout.Nanoseconds()},
	}
	_, err := d.client.Request(protocol.OpExLockWithTimeout, m)
	return err
}

// Unlock releases an acquired lock for the given key. It returns olric.ErrNoSuchLock if there is no lock for the given key.
func (d *DMap) Unlock(key string) error {
	m := &protocol.Message{
		DMap: d.name,
		Key:  key,
	}
	resp, err := d.client.Request(protocol.OpExUnlock, m)
	if resp.Status == protocol.StatusNoSuchLock {
		return olric.ErrNoSuchLock
	}
	return err
}

// Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.
// So if you call Put/PutEx and Destroy methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.
func (d *DMap) Destroy() error {
	m := &protocol.Message{
		DMap: d.name,
	}
	_, err := d.client.Request(protocol.OpExDestroy, m)
	return err
}

func (c *Client) incrDecr(op protocol.OpCode, name, key string, delta int) (int, error) {
	value, err := c.serializer.Marshal(delta)
	if err != nil {
		return 0, err
	}
	m := &protocol.Message{
		DMap:  name,
		Key:   key,
		Value: value,
	}
	resp, err := c.client.Request(op, m)
	if err != nil {
		return 0, err
	}
	var res interface{}
	err = c.serializer.Unmarshal(resp.Value, &res)
	return res.(int), err
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (d *DMap) Incr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpExIncr, d.name, key, delta)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (d *DMap) Decr(key string, delta int) (int, error) {
	return d.incrDecr(protocol.OpExDecr, d.name, key, delta)
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (d *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	data, err := d.serializer.Marshal(value)
	if err != nil {
		return nil, err
	}
	m := &protocol.Message{
		DMap:  d.name,
		Key:   key,
		Value: data,
	}
	resp, err := d.client.Request(protocol.OpExGetPut, m)
	if err != nil {
		return nil, err
	}
	var oldval interface{}
	if len(resp.Value) != 0 {
		err = d.serializer.Unmarshal(resp.Value, &oldval)
		if err != nil {
			return nil, err
		}
	}
	return oldval, nil
}
