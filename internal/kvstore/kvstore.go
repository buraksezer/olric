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

/*Package kvstore implements a GC friendly in-memory storage engine by using
built-in maps and byte slices. It also supports compaction.*/
package kvstore

import (
	"errors"
	"log"
	"regexp"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

const (
	maxGarbageRatio = 0.40
	// 1MB
	defaultTableSize = uint32(1 << 20)

	defaultMaxIdleTableTimeout = 15 * time.Minute
)

// KVStore implements an in-memory storage engine.
type KVStore struct {
	tables []*table.Table
	config *storage.Config
}

var _ storage.Engine = (*KVStore)(nil)

func DefaultConfig() *storage.Config {
	options := storage.NewConfig(nil)
	options.Add("tableSize", defaultTableSize)
	options.Add("maxIdleTableTimeout", defaultMaxIdleTableTimeout)
	return options
}

func (k *KVStore) SetConfig(c *storage.Config) {
	k.config = c
}

func (k *KVStore) makeTable() error {
	size, err := k.config.Get("tableSize")
	if err != nil {
		return err
	}

	head := k.tables[len(k.tables)-1]
	head.SetState(table.ReadOnlyState)

	for i, t := range k.tables {
		if t.State() == table.RecycledState {
			k.tables = append(k.tables, t)
			k.tables = append(k.tables[:i], k.tables[i+1:]...)
			t.SetState(table.ReadWriteState)
			return nil
		}
	}

	current := table.New(size.(uint32))
	k.tables = append(k.tables, current)
	return nil
}

func (k *KVStore) SetLogger(_ *log.Logger) {}

func (k *KVStore) Start() error {
	if k.config == nil {
		return errors.New("config cannot be nil")
	}
	return nil
}

// Fork creates a new KVStore instance.
func (k *KVStore) Fork(c *storage.Config) (storage.Engine, error) {
	if c == nil {
		c = k.config.Copy()
	}
	size, err := c.Get("tableSize")
	if err != nil {
		return nil, err
	}
	child := &KVStore{
		config: c,
	}
	t := table.New(size.(uint32))
	child.tables = append(child.tables, t)
	return child, nil
}

func (k *KVStore) AppendTable(t *table.Table) {
	k.tables = append(k.tables, t)
}

func (k *KVStore) Name() string {
	return "kvstore"
}

func (k *KVStore) NewEntry() storage.Entry {
	return entry.New()
}

// PutRaw sets the raw value for the given key.
func (k *KVStore) PutRaw(hkey uint64, value []byte) error {
	if len(k.tables) == 0 {
		if err := k.makeTable(); err != nil {
			return err
		}
	}

	for {
		// Get the last value, storage only calls Put on the last created table.
		t := k.tables[len(k.tables)-1]
		err := t.PutRaw(hkey, value)
		if errors.Is(err, table.ErrNotEnoughSpace) {
			err := k.makeTable()
			if err != nil {
				return err
			}
			// try again
			continue
		}
		if err != nil {
			return err
		}
		// everything is ok
		break
	}

	return nil
}

// Put sets the value for the given key. It overwrites any previous value for that key
func (k *KVStore) Put(hkey uint64, value storage.Entry) error {
	if len(k.tables) == 0 {
		if err := k.makeTable(); err != nil {
			return err
		}
	}

	for {
		// Get the last value, storage only calls Put on the last created table.
		t := k.tables[len(k.tables)-1]
		err := t.Put(hkey, value)
		if errors.Is(err, table.ErrNotEnoughSpace) {
			err := k.makeTable()
			if err != nil {
				return err
			}
			// try again
			continue
		}
		if err != nil {
			return err
		}

		// everything is ok
		break
	}

	return nil
}

// GetRaw extracts encoded value for the given hkey. This is useful for merging tables.
func (k *KVStore) GetRaw(hkey uint64) ([]byte, error) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		raw, err := t.GetRaw(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return nil, err
		}
		// Found the key, return the stored value with its metadata.
		return raw, nil
	}

	// Nothing here.
	return nil, storage.ErrKeyNotFound
}

// Get gets the value for the given key. It returns storage.ErrKeyNotFound if the DB
// does not contain the key. The returned Entry is its own copy,
// it is safe to modify the contents of the returned slice.
func (k *KVStore) Get(hkey uint64) (storage.Entry, error) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		res, err := t.Get(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return nil, err
		}
		// Found the key, return the stored value with its metadata.
		return res, nil
	}
	// Nothing here.
	return nil, storage.ErrKeyNotFound
}

// GetTTL gets the timeout for the given key. It returns storage.ErrKeyNotFound if the DB
// does not contain the key.
func (k *KVStore) GetTTL(hkey uint64) (int64, error) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		ttl, err := t.GetTTL(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return 0, err
		}
		// Found the key, return its ttl
		return ttl, nil
	}

	// Nothing here.
	return 0, storage.ErrKeyNotFound
}

func (k *KVStore) GetLastAccess(hkey uint64) (int64, error) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		lastAccess, err := t.GetLastAccess(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return 0, err
		}
		// Found the key, return its ttl
		return lastAccess, nil
	}

	// Nothing here.
	return 0, storage.ErrKeyNotFound
}

// GetKey gets the key for the given hkey. It returns storage.ErrKeyNotFound if the DB
// does not contain the key.
func (k *KVStore) GetKey(hkey uint64) (string, error) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		key, err := t.GetKey(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return "", err
		}
		// Found the key, return its ttl
		return key, nil
	}

	// Nothing here.
	return "", storage.ErrKeyNotFound
}

// Delete deletes the value for the given key. Delete will not returns error if key doesn't exist.
func (k *KVStore) Delete(hkey uint64) error {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		err := t.Delete(hkey)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return err
		}
		break
	}

	return nil
}

// UpdateTTL updates the expiry for the given key.
func (k *KVStore) UpdateTTL(hkey uint64, data storage.Entry) error {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		err := t.UpdateTTL(hkey, data)
		if errors.Is(err, table.ErrHKeyNotFound) {
			// Try out the other tables.
			continue
		}
		if err != nil {
			return err
		}
		// Found the key, return the stored value with its metadata.
		return nil
	}
	// Nothing here.
	return storage.ErrKeyNotFound
}

// Stats is a function which provides memory allocation and garbage ratio of a storage instance.
func (k *KVStore) Stats() storage.Stats {
	stats := storage.Stats{
		NumTables: len(k.tables),
	}
	for _, t := range k.tables {
		s := t.Stats()
		stats.Allocated += int(s.Allocated)
		stats.Inuse += int(s.Inuse)
		stats.Garbage += int(s.Garbage)
		stats.Length += s.Length
	}
	return stats
}

// Check checks the key existence.
func (k *KVStore) Check(hkey uint64) bool {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		ok := t.Check(hkey)
		if ok {
			return true
		}
	}

	// Nothing there.
	return false
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration. Range may be O(N) with
// the number of elements in the map even if f returns false after a constant
// number of calls.
func (k *KVStore) Range(f func(hkey uint64, e storage.Entry) bool) {
	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		t.Range(func(hkey uint64, e storage.Entry) bool {
			return f(hkey, e)
		})
	}
}

// RegexMatchOnKeys calls a regular expression on keys and provides an iterator.
func (k *KVStore) RegexMatchOnKeys(expr string, f func(hkey uint64, e storage.Entry) bool) error {
	if len(k.tables) == 0 {
		// There is nothing to do
		return nil
	}

	r, err := regexp.Compile(expr)
	if err != nil {
		return err
	}

	// Scan available tables by starting the last added table.
	for i := len(k.tables) - 1; i >= 0; i-- {
		t := k.tables[i]
		t.Range(func(hkey uint64, e storage.Entry) bool {
			key, _ := t.GetRawKey(hkey)
			if !r.Match(key) {
				return true
			}
			data, _ := t.Get(hkey)
			return f(hkey, data)
		})
	}

	return nil
}

func (k *KVStore) Close() error {
	return nil
}

func (k *KVStore) Destroy() error {
	return nil
}
