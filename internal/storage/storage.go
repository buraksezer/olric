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

/*Package storage implements a GC friendly in-memory storage engine by using map and byte array. It also supports compaction.*/
package storage

import (
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const (
	maxGarbageRatio = 0.40
	// 65kb
	minimumSize = 1 << 16
)

// ErrFragmented is an error that indicates this storage instance is currently
// fragmented and it cannot be serialized.
var ErrFragmented = errors.New("storage fragmented")

// SlabInfo is used to expose internal data usage of a storage instance.
type SlabInfo struct {
	Allocated int
	Inuse     int
	Garbage   int
}

// VData represents a value with its metadata.
type VData struct {
	Key       string
	Value     []byte
	TTL       int64
	Timestamp int64
}

// Storage implements a new off-heap data store which uses built-in map to
// keep metadata and mmap syscall for allocating memory to store values.
// The allocated memory is not a subject of Golang's GC.
type Storage struct {
	tables []*table
}

// New creates a new storage instance.
func New(size int) *Storage {
	str := &Storage{}
	t := newTable(size)
	str.tables = append(str.tables, t)
	return str
}

// PutRaw sets the raw value for the given key.
func (s *Storage) PutRaw(hkey uint64, value []byte) error {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	var res error
	for {
		// Get the last value, storage only calls Put on the last created table.
		t := s.tables[len(s.tables)-1]
		err := t.putRaw(hkey, value)
		if err == errNotEnoughSpace {
			// Create a new table and put the new k/v pair in it.
			nt := newTable(s.Inuse() * 2)
			s.tables = append(s.tables, nt)
			res = ErrFragmented
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

// Put sets the value for the given key. It overwrites any previous value for that key
func (s *Storage) Put(hkey uint64, value *VData) error {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	var res error
	for {
		// Get the last value, storage only calls Put on the last created table.
		t := s.tables[len(s.tables)-1]
		err := t.put(hkey, value)
		if err == errNotEnoughSpace {
			// Create a new table and put the new k/v pair in it.
			nt := newTable(s.Inuse() * 2)
			s.tables = append(s.tables, nt)
			res = ErrFragmented
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
func (s *Storage) GetRaw(hkey uint64) ([]byte, error) {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		rawval, prev := t.getRaw(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return rawval, nil
	}

	// Nothing here.
	return nil, ErrKeyNotFound
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key. The returned VData is its own copy,
// it is safe to modify the contents of the returned slice.
func (s *Storage) Get(hkey uint64) (*VData, error) {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		res, prev := t.get(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return res, nil
	}
	// Nothing here.
	return nil, ErrKeyNotFound
}

// GetTTL gets the timeout for the given key. It returns ErrKeyNotFound if the DB
// does not contains the key.
func (s *Storage) GetTTL(hkey uint64) (int64, error) {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		ttl, prev := t.getTTL(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return its ttl
		return ttl, nil
	}
	// Nothing here.
	return 0, ErrKeyNotFound
}

// GetKey gets the key for the given hkey. It returns ErrKeyNotFound if the DB
// does not contains the key.
func (s *Storage) GetKey(hkey uint64) (string, error) {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		key, prev := t.getKey(hkey)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return its ttl
		return key, nil
	}
	// Nothing here.
	return "", ErrKeyNotFound
}

// Delete deletes the value for the given key. Delete will not returns error if key doesn't exist.
func (s *Storage) Delete(hkey uint64) error {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		if prev := t.delete(hkey); prev {
			// Try out the other tables.
			continue
		}
		break
	}

	if len(s.tables) != 1 {
		return nil
	}

	t := s.tables[0]
	if float64(t.allocated)*maxGarbageRatio <= float64(t.garbage) {
		// Create a new table here.
		nt := newTable(s.Inuse() * 2)
		s.tables = append(s.tables, nt)
		return ErrFragmented
	}
	return nil
}

// UpdateTTL updates the expiry for the given key.
func (s *Storage) UpdateTTL(hkey uint64, data *VData) error {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		prev := t.updateTTL(hkey, data)
		if prev {
			// Try out the other tables.
			continue
		}
		// Found the key, return the stored value with its metadata.
		return nil
	}
	// Nothing here.
	return ErrKeyNotFound
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
func (s *Storage) Export() ([]byte, error) {
	if len(s.tables) != 1 {
		return nil, ErrFragmented
	}
	t := s.tables[0]
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
func Import(data []byte) (*Storage, error) {
	tr := transport{}
	err := msgpack.Unmarshal(data, &tr)
	if err != nil {
		return nil, err
	}

	o := New(tr.Allocated)

	t := o.tables[0]
	t.hkeys = tr.HKeys
	t.offset = tr.Offset
	t.inuse = tr.Inuse
	t.garbage = tr.Garbage
	copy(t.memory, tr.Memory)
	return o, nil
}

// Len returns the key cound in this storage.
func (s *Storage) Len() int {
	var total int
	for _, t := range s.tables {
		total += len(t.hkeys)
	}
	return total
}

// SlabInfo is a function which provides memory allocation
// and garbage ratio of a storage instance.
func (s *Storage) SlabInfo() SlabInfo {
	si := SlabInfo{}
	for _, t := range s.tables {
		si.Allocated += t.allocated
		si.Inuse += t.inuse
		si.Garbage += t.garbage
	}
	return si
}

// Inuse returns total in-use space by the tables.
func (s *Storage) Inuse() int {
	// SlabInfo does the same thing but we need
	// to eliminate useless calls.
	inuse := 0
	for _, t := range s.tables {
		inuse += t.inuse
	}
	return inuse
}

// Check checks the key existence.
func (s *Storage) Check(hkey uint64) bool {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
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
func (s *Storage) Range(f func(hkey uint64, vdata *VData) bool) {
	if len(s.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(s.tables) - 1; i >= 0; i-- {
		t := s.tables[i]
		for hkey := range t.hkeys {
			vdata, _ := t.get(hkey)
			if !f(hkey, vdata) {
				break
			}
		}
	}
}
