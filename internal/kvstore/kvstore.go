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
	"fmt"
	"log"
	"regexp"

	"github.com/buraksezer/olric/pkg/storage"
	"github.com/vmihailenco/msgpack"
)

const (
	maxGarbageRatio = 0.40
	// 1MB
	defaultTableSize = 1 << 20
)

// KVStore implements a new off-heap data store which uses built-in map to
// keep metadata and mmap syscall for allocating memory to store values.
// The allocated memory is not a subject of Golang's GC.
type KVStore struct {
	minimumTableSize int
	tables           []*table
	config           *storage.Config
}

var _ storage.Engine = (*KVStore)(nil)

func DefaultConfig() *storage.Config {
	options := storage.NewConfig(nil)
	options.Add("tableSize", defaultTableSize)
	return options
}

func SanitizeConfig(cfg map[string]interface{}) map[string]interface{} {
	if _, ok := cfg["tableSize"]; !ok {
		cfg["tableSize"] = defaultTableSize
	}
	return cfg
}

func (kv *KVStore) SetConfig(c *storage.Config) {
	kv.config = c
}

func (kv *KVStore) SetLogger(l *log.Logger) {}

func (kv *KVStore) Start() error {
	if kv.config == nil {
		return errors.New("config cannot be nil")
	}
	return nil
}

// Fork creates a new KVStore instance.
func (kv *KVStore) Fork(c *storage.Config) (storage.Engine, error) {
	if c == nil {
		c = kv.config.Copy()
	}
	size, err := c.Get("tableSize")
	if err != nil {
		return nil, err
	}

	if _, ok := size.(int); !ok {
		return nil, fmt.Errorf("tableSize is %T, not int", size)
	}

	child := &KVStore{
		config:           c,
		minimumTableSize: size.(int),
	}
	t := newTable(child.minimumTableSize)
	child.tables = append(child.tables, t)
	return child, nil
}

func (kv *KVStore) Name() string {
	return "kvstore"
}

func (kv *KVStore) NewEntry() storage.Entry {
	return NewEntry()
}

// PutRaw sets the raw value for the given key.
func (kv *KVStore) PutRaw(hkey uint64, value []byte) error {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	var res error
	for {
		// Get the last value, storage only calls Put on the last created table.
		t := kv.tables[len(kv.tables)-1]
		err := t.putRaw(hkey, value)
		if err == errNotEnoughSpace {
			// Create a new table and put the new k/v pair in it.
			// The value includes its metadata, so there is no need to use requiredSpaceForAnEntry.
			ntSize := kv.calculateTableSize(len(value))
			nt := newTable(ntSize)
			kv.tables = append(kv.tables, nt)
			res = storage.ErrFragmented
			// try again
			continue
		}
		if err != nil {
			return err
		}
		// everything is ok
		break
	}
	return res
}

// requiredSpaceForAnEntry calculates how many bytes are required to store an storage.Entry.
func requiredSpaceForAnEntry(keyLength, valueLength int) int {
	return keyLength + valueLength + metadataLen
}

// calculateTableSize calculates a new table size to expand the underlying table. If you use
// zero (0) as minimumRequiredSize, it returns the default table size.
func (kv *KVStore) calculateTableSize(minimumRequiredSize int) int {
	s := kv.Stats()
	inuse := s.Inuse
	newTableSize := (inuse + minimumRequiredSize) * 2

	// Minimum table size is kv.minimumTableSize
	if newTableSize < kv.minimumTableSize {
		return kv.minimumTableSize
	}

	// Expand the table.
	return newTableSize
}

// Put sets the value for the given key. It overwrites any previous value for that key
func (kv *KVStore) Put(hkey uint64, value storage.Entry) error {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}
	var res error
	for {
		// Get the last value, storage only calls Put on the last created table.
		t := kv.tables[len(kv.tables)-1]
		err := t.put(hkey, value)
		if err == errNotEnoughSpace {
			minimumRequiredSpace := requiredSpaceForAnEntry(len(value.Key()), len(value.Value()))
			ntSize := kv.calculateTableSize(minimumRequiredSpace)

			// Create a new table and put the new k/v pair in it.
			nt := newTable(ntSize)
			kv.tables = append(kv.tables, nt)
			res = storage.ErrFragmented
			// try again
			continue
		}
		if err != nil {
			return err
		}
		// everything is ok
		break
	}
	return res
}

// GetRaw extracts encoded value for the given hkey. This is useful for merging tables.
func (kv *KVStore) GetRaw(hkey uint64) ([]byte, error) {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		rawval, prev := t.getRaw(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return rawval, nil
	}

	// Nothing here.
	return nil, storage.ErrKeyNotFound
}

// Get gets the value for the given key. It returns storage.ErrKeyNotFound if the DB
// does not contains the key. The returned Entry is its own copy,
// it is safe to modify the contents of the returned slice.
func (kv *KVStore) Get(hkey uint64) (storage.Entry, error) {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		res, prev := t.get(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return res, nil
	}
	// Nothing here.
	return nil, storage.ErrKeyNotFound
}

// GetTTL gets the timeout for the given key. It returns storage.ErrKeyNotFound if the DB
// does not contains the key.
func (kv *KVStore) GetTTL(hkey uint64) (int64, error) {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		ttl, prev := t.getTTL(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return its ttl
		return ttl, nil
	}
	// Nothing here.
	return 0, storage.ErrKeyNotFound
}

// GetKey gets the key for the given hkey. It returns storage.ErrKeyNotFound if the DB
// does not contains the key.
func (kv *KVStore) GetKey(hkey uint64) (string, error) {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		key, prev := t.getKey(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return its ttl
		return key, nil
	}
	// Nothing here.
	return "", storage.ErrKeyNotFound
}

// Delete deletes the value for the given key. Delete will not returns error if key doesn't exist.
func (kv *KVStore) Delete(hkey uint64) error {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		if prev := t.delete(hkey); prev {
			// Try out the other tables.
			continue
		}
		break
	}

	if len(kv.tables) != 1 {
		return nil
	}

	t := kv.tables[0]
	if float64(t.allocated)*maxGarbageRatio <= float64(t.garbage) {
		// Create a new table here.
		nt := newTable(kv.minimumTableSize)
		kv.tables = append(kv.tables, nt)
		return storage.ErrFragmented
	}
	return nil
}

// UpdateTTL updates the expiry for the given key.
func (kv *KVStore) UpdateTTL(hkey uint64, data storage.Entry) error {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		prev := t.updateTTL(hkey, data)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return nil
	}
	// Nothing here.
	return storage.ErrKeyNotFound
}

type transport struct {
	HKeys     map[uint64]int
	Memory    []byte
	Offset    int
	Allocated int
	Inuse     int
	Garbage   int
}

// Export serializes underlying data structes into a byte slice. It may return
// ErrFragmented if the tables are fragmented. If you get this error, you should
// try to call Export again some time later.
func (kv *KVStore) Export() ([]byte, error) {
	if len(kv.tables) != 1 {
		return nil, storage.ErrFragmented
	}
	t := kv.tables[0]
	tr := &transport{
		HKeys:     t.hkeys,
		Offset:    t.offset,
		Allocated: t.allocated,
		Inuse:     t.inuse,
		Garbage:   t.garbage,
	}
	tr.Memory = make([]byte, t.offset+1)
	copy(tr.Memory, t.memory[:t.offset])
	return msgpack.Marshal(tr)
}

// Import gets the serialized data by Export and creates a new storage instance.
func (kv *KVStore) Import(data []byte) (storage.Engine, error) {
	tr := transport{}
	err := msgpack.Unmarshal(data, &tr)
	if err != nil {
		return nil, err
	}

	c := kv.config.Copy()
	c.Add("tableSize", tr.Allocated)

	child, err := kv.Fork(c)
	if err != nil {
		return nil, err
	}
	t := child.(*KVStore).tables[0]
	t.hkeys = tr.HKeys
	t.offset = tr.Offset
	t.inuse = tr.Inuse
	t.garbage = tr.Garbage
	copy(t.memory, tr.Memory)
	return child, nil
}

// Stats is a function which provides memory allocation and garbage ratio of a storage instance.
func (kv *KVStore) Stats() storage.Stats {
	stats := storage.Stats{
		NumTables: len(kv.tables),
	}
	for _, t := range kv.tables {
		stats.Allocated += t.allocated
		stats.Inuse += t.inuse
		stats.Garbage += t.garbage
		stats.Length += len(t.hkeys)
	}
	return stats
}

// Check checks the key existence.
func (kv *KVStore) Check(hkey uint64) bool {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		_, ok := t.hkeys[hkey]
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
func (kv *KVStore) Range(f func(hkey uint64, entry storage.Entry) bool) {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		for hkey := range t.hkeys {
			entry, _ := t.get(hkey)
			if !f(hkey, entry) {
				break
			}
		}
	}
}

// RegexMatchOnKeys calls a regular expression on keys and provides an iterator.
func (kv *KVStore) RegexMatchOnKeys(expr string, f func(hkey uint64, entry storage.Entry) bool) error {
	if len(kv.tables) == 0 {
		panic("tables cannot be empty")
	}
	r, err := regexp.Compile(expr)
	if err != nil {
		return err
	}

	// Scan available tables by starting the last added table.
	for i := len(kv.tables) - 1; i >= 0; i-- {
		t := kv.tables[i]
		for hkey := range t.hkeys {
			key, _ := t.getRawKey(hkey)
			if !r.Match(key) {
				continue
			}
			data, _ := t.get(hkey)
			if !f(hkey, data) {
				return nil
			}
		}
	}
	return nil
}

func (kv *KVStore) Close() error {
	return nil
}

func (kv *KVStore) Destroy() error {
	return nil
}
