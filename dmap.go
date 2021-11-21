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

package olric

import (
	"errors"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/query"
)

const (
	IfNotFound = int16(1) << iota
	IfFound
)

var (
	// ErrKeyNotFound means that returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrKeyFound means that the requested key found in the cluster.
	ErrKeyFound = errors.New("key found")

	// ErrWriteQuorum means that write quorum cannot be reached to operate.
	ErrWriteQuorum = errors.New("write quorum cannot be reached")

	// ErrReadQuorum means that read quorum cannot be reached to operate.
	ErrReadQuorum = errors.New("read quorum cannot be reached")

	// ErrLockNotAcquired is returned when the requested lock could not be acquired
	ErrLockNotAcquired = errors.New("lock not acquired")

	// ErrNoSuchLock is returned when the requested lock does not exist
	ErrNoSuchLock = errors.New("no such lock")

	// ErrEndOfQuery is the error returned by Range when no more data is available.
	// Functions should return ErrEndOfQuery only to signal a graceful end of input.
	ErrEndOfQuery = errors.New("end of query")

	// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
	ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

	// ErrKeyTooLarge means that the given key is too large to process.
	// Maximum length of a key is 256 bytes.
	ErrKeyTooLarge = errors.New("key too large")
)

// NumConcurrentWorkers is the number of concurrent workers to run a query on the cluster.
const NumConcurrentWorkers = 2

// QueryResponse denotes returned data by a node for query.
type QueryResponse map[string]interface{}

func convertDMapError(err error) error {
	switch {
	case errors.Is(err, dmap.ErrKeyFound):
		return ErrKeyFound
	case errors.Is(err, dmap.ErrKeyNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrDMapNotFound):
		return ErrKeyNotFound
	case errors.Is(err, dmap.ErrEndOfQuery):
		return ErrEndOfQuery
	case errors.Is(err, dmap.ErrLockNotAcquired):
		return ErrLockNotAcquired
	case errors.Is(err, dmap.ErrNoSuchLock):
		return ErrNoSuchLock
	case errors.Is(err, dmap.ErrReadQuorum):
		return ErrReadQuorum
	case errors.Is(err, dmap.ErrWriteQuorum):
		return ErrWriteQuorum
	case errors.Is(err, dmap.ErrServerGone):
		return ErrServerGone
	default:
		return convertClusterError(err)
	}
}

// Entry is a DMap entry with its metadata.
type Entry struct {
	Key       string
	Value     interface{}
	TTL       int64
	Timestamp int64
}

// LockContext is returned by Lock and LockWithTimeout methods.
// It should be stored in a proper way to release the lock.
type LockContext struct {
	ctx *dmap.LockContext
}

// Cursor implements distributed query on DMaps.
type Cursor struct {
	cursor *dmap.Cursor
}

// DMap represents a distributed map instance.
type DMap struct {
	dm *dmap.DMap
}

// NewDMap creates an returns a new DMap instance.
func (db *Olric) NewDMap(name string) (*DMap, error) {
	dm, err := db.dmap.NewDMap(name)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &DMap{
		dm: dm,
	}, nil
}

// Name exposes name of the DMap.
func (dm *DMap) Name() string {
	return dm.dm.Name()
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contain the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) Get(key string) (interface{}, error) {
	value, err := dm.dm.Get(key)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return value, nil
}

// GetEntry gets the value for the given key with its metadata. It returns ErrKeyNotFound if the DB
// does not contain the key. It's thread-safe. It is safe to modify the contents
// of the returned value.
func (dm *DMap) GetEntry(key string) (*Entry, error) {
	e, err := dm.dm.GetEntry(key)
	if err != nil {
		return nil, convertDMapError(err)
	}

	return &Entry{
		Key:       e.Key,
		Value:     e.Value,
		TTL:       e.TTL,
		Timestamp: e.Timestamp,
	}, nil
}

// LockWithTimeout sets a lock for the given key. If the lock is still unreleased the end of given period of time,
// it automatically releases the lock. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) LockWithTimeout(key string, timeout, deadline time.Duration) (*LockContext, error) {
	ctx, err := dm.dm.LockWithTimeout(key, timeout, deadline)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &LockContext{ctx: ctx}, nil
}

// Lock sets a lock for the given key. Acquired lock is only for the key in this dmap.
//
// It returns immediately if it acquires the lock for the given key. Otherwise, it waits until deadline.
//
// You should know that the locks are approximate, and only to be used for non-critical purposes.
func (dm *DMap) Lock(key string, deadline time.Duration) (*LockContext, error) {
	ctx, err := dm.dm.Lock(key, deadline)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &LockContext{ctx: ctx}, nil
}

// Unlock releases the lock.
func (l *LockContext) Unlock() error {
	err := l.ctx.Unlock()
	return convertDMapError(err)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous
// value for that key. It's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	err := dm.dm.PutEx(key, value, timeout)
	return convertDMapError(err)
}

// Put sets the value for the given key. It overwrites any previous value
// for that key, and it's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
func (dm *DMap) Put(key string, value interface{}) error {
	err := dm.dm.Put(key, value)
	return convertDMapError(err)
}

// PutIf sets the value for the given key. It overwrites any previous value
// for that key, and it's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
// Flag argument currently has two different options:
//
// IfNotFound: Only set the key if it does not already exist.
// It returns ErrFound if the key already exist.
//
// IfFound: Only set the key if it already exist.
// It returns ErrKeyNotFound if the key does not exist.
func (dm *DMap) PutIf(key string, value interface{}, flags int16) error {
	err := dm.dm.PutIf(key, value, flags)
	return convertDMapError(err)
}

// PutIfEx sets the value for the given key with TTL. It overwrites any previous
// value for that key. It's thread-safe. The key has to be string. value type
// is arbitrary. It is safe to modify the contents of the arguments after
// Put returns but not before.
// Flag argument currently has two different options:
//
// IfNotFound: Only set the key if it does not already exist.
// It returns ErrFound if the key already exist.
//
// IfFound: Only set the key if it already exist.
// It returns ErrKeyNotFound if the key does not exist.
func (dm *DMap) PutIfEx(key string, value interface{}, timeout time.Duration, flags int16) error {
	err := dm.dm.PutIfEx(key, value, timeout, flags)
	return convertDMapError(err)
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contain the key. It's thread-safe.
func (dm *DMap) Expire(key string, timeout time.Duration) error {
	err := dm.dm.Expire(key, timeout)
	return convertDMapError(err)
}

// Query runs a distributed query on a dmap instance.
// Olric supports a very simple query DSL and now, it only scans keys. The query DSL has very
// few keywords:
//
// $onKey: Runs the given query on keys or manages options on keys for a given query.
//
// $onValue: Runs the given query on values or manages options on values for a given query.
//
// $options: Useful to modify data returned from a query
//
// Keywords for $options:
//
// $ignore: Ignores a value.
//
// A distributed query looks like the following:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "^even:",
// 		  "$options": query.M{
// 			  "$onValue": query.M{
// 				  "$ignore": true,
// 			  },
// 		  },
// 	  },
//   }
//
// This query finds the keys starts with "even:", drops the values and returns only keys.
// If you also want to retrieve the values, just remove the $options directive:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "^even:",
// 	  },
//   }
//
// In order to iterate over all the keys:
//
//   query.M{
// 	  "$onKey": query.M{
// 		  "$regexMatch": "",
// 	  },
//   }
//
// Query function returns a cursor which has Range and Close methods. Please take look at the Range
// function for further info.
func (dm *DMap) Query(q query.M) (*Cursor, error) {
	c, err := dm.dm.Query(q)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return &Cursor{cursor: c}, nil
}

// Range calls f sequentially for each key and value yielded from the cursor. If f returns false,
// range stops the iteration.
func (c *Cursor) Range(f func(key string, value interface{}) bool) error {
	err := c.cursor.Range(f)
	return convertDMapError(err)
}

// Close cancels the underlying context and background goroutines stops running.
func (c *Cursor) Close() {
	c.cursor.Close()
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(key string) error {
	err := dm.dm.Delete(key)
	return convertDMapError(err)
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (dm *DMap) Incr(key string, delta int) (int, error) {
	value, err := dm.dm.Incr(key, delta)
	if err != nil {
		return 0, convertDMapError(err)
	}
	return value, nil
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (dm *DMap) Decr(key string, delta int) (int, error) {
	value, err := dm.dm.Decr(key, delta)
	if err != nil {
		return 0, convertDMapError(err)
	}
	return value, nil
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (dm *DMap) GetPut(key string, value interface{}) (interface{}, error) {
	prev, err := dm.dm.GetPut(key, value)
	if err != nil {
		return nil, convertDMapError(err)
	}
	return prev, nil
}

// Destroy flushes the given DMap on the cluster. You should know that there
// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
// concurrently on the cluster, Put/PutEx calls may set new values to the dmap.
func (dm *DMap) Destroy() error {
	err := dm.dm.Destroy()
	return convertDMapError(err)
}
