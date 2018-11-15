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

package offheap

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const (
	maxGarbageRatio = 0.40
	// 1MB
	minimumSize = 1 << 20
)

// ErrFragmented is an error that indicates this offheap instance is currently
// fragmented and it cannot be serialized.
var ErrFragmented = errors.New("offheap fragmented")

// VData represents a value with its metadata.
type VData struct {
	Key   string
	TTL   int64
	Value []byte
}

// Offheap implements a new off-heap data store which uses built-in map to
// keep metadata and mmap syscall for allocating memory to store values.
// The allocated memory is not a subject of Golang's GC.
type Offheap struct {
	mu sync.RWMutex

	tables  []*table
	merging int32
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// New creates a new Offheap instance.
func New(size int) (*Offheap, error) {
	ctx, cancel := context.WithCancel(context.Background())
	o := &Offheap{
		ctx:    ctx,
		cancel: cancel,
	}
	t, err := newTable(size)
	if err != nil {
		return nil, err
	}
	o.tables = append(o.tables, t)
	return o, nil
}

// Close closes underlying tables and releases allocated memory with Munmap.
// It blocks until everything is done.
func (o *Offheap) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	select {
	case <-o.ctx.Done():
		// It's already closed.
		return nil
	default:
	}

	o.cancel()
	// Await for table merging processes gets closed.
	o.wg.Wait()

	// free allocated area with Munmap.
	for _, t := range o.tables {
		err := t.close()
		if err != nil {
			return err
		}
	}
	// Olric can be used as an embedded database, so closing an offheap
	// instance or Olric's itself, doesn't mean closing the process.
	// GC will throw out the metadata.
	o.tables = nil
	return nil
}

// PutRaw sets the raw value for the given key.
func (o *Offheap) PutRaw(hkey uint64, value []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	for {
		// Get the last value, offheap only calls Put on the last created table.
		t := o.tables[len(o.tables)-1]
		err := t.putRaw(hkey, value)
		if err == errNotEnoughSpace {
			// Create a new table and put the new k/v pair in it.
			nt, err := newTable(t.inuse * 2)
			if err != nil {
				return err
			}
			o.tables = append(o.tables, nt)
			if atomic.LoadInt32(&o.merging) == 0 {
				o.wg.Add(1)
				atomic.StoreInt32(&o.merging, 1)
				go o.mergeTables()
			}
			continue
		}
		// returns an error or nil.
		return err
	}
}

// Put sets the value for the given key. It overwrites any previous value for that key
func (o *Offheap) Put(hkey uint64, value *VData) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	for {
		// Get the last value, offheap only calls Put on the last created table.
		t := o.tables[len(o.tables)-1]
		err := t.put(hkey, value)
		if err == errNotEnoughSpace {
			// Create a new table and put the new k/v pair in it.
			nt, err := newTable(t.inuse * 2)
			if err != nil {
				return err
			}
			o.tables = append(o.tables, nt)
			if atomic.LoadInt32(&o.merging) == 0 {
				o.wg.Add(1)
				atomic.StoreInt32(&o.merging, 1)
				go o.mergeTables()
			}
			continue
		}
		// returns an error or nil.
		return err
	}
}

// GetRaw extracts un-decoded value for the given hkey. This is useful for merging tables or
// snapshots.
func (o *Offheap) GetRaw(hkey uint64) ([]byte, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(o.tables) - 1; i >= 0; i-- {
		t := o.tables[i]
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
func (o *Offheap) Get(hkey uint64) (*VData, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(o.tables) - 1; i >= 0; i-- {
		t := o.tables[i]
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

// Delete deletes the value for the given key. Delete will not returns error if key doesn't exist.
func (o *Offheap) Delete(hkey uint64) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(o.tables) - 1; i >= 0; i-- {
		t := o.tables[i]
		if prev := t.delete(hkey); prev {
			// Try out the other tables.
			continue
		}
		break
	}

	//Check garbage ratio here, create a new table if you need.
	if len(o.tables) != 1 {
		return nil
	}
	t := o.tables[0]
	if float64(t.allocated)*maxGarbageRatio <= float64(t.garbage) {
		if atomic.LoadInt32(&o.merging) == 1 {
			return nil
		}
		// Create a new table and put the new k/v pair in it.
		newSize := t.inuse * 2
		if newSize > t.allocated {
			// Don't grow up.
			newSize = t.allocated
		}
		nt, err := newTable(newSize)
		if err != nil {
			return err
		}
		o.tables = append(o.tables, nt)
		o.wg.Add(1)
		atomic.StoreInt32(&o.merging, 1)
		go o.mergeTables()
	}
	return nil
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
func (o *Offheap) Export() ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.tables) != 1 {
		return nil, ErrFragmented
	}
	t := o.tables[0]
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

// Import gets the serialized data by Export and creates a new Offheap instance.
func Import(data []byte) (*Offheap, error) {
	tr := transport{}
	err := msgpack.Unmarshal(data, &tr)
	if err != nil {
		return nil, err
	}

	o, err := New(tr.Allocated)
	if err != nil {
		return nil, err
	}

	t := o.tables[0]
	t.hkeys = tr.HKeys
	t.offset = tr.Offset
	t.inuse = tr.Inuse
	t.garbage = tr.Garbage
	copy(t.memory, tr.Memory)
	return o, nil
}

// Len returns the key cound in this Offheap.
func (o *Offheap) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	var total int
	for _, t := range o.tables {
		total += len(t.hkeys)
	}
	return total
}

// Check checks the key existence.
func (o *Offheap) Check(hkey uint64) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(o.tables) - 1; i >= 0; i-- {
		t := o.tables[i]
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
func (o *Offheap) Range(f func(hkey uint64, vdata *VData) bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if len(o.tables) == 0 {
		panic("tables cannot be empty")
	}

	// Scan available tables by starting the last added table.
	for i := len(o.tables) - 1; i >= 0; i-- {
		t := o.tables[i]
		for hkey := range t.hkeys {
			vdata, _ := t.get(hkey)
			if !f(hkey, vdata) {
				break
			}
		}
	}
}

// DecodeRaw creates VData for given byte slice. It assumes that the given data is valid. Never returns an error.
func DecodeRaw(raw []byte) *VData {
	offset := 0
	vdata := &VData{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(uint8(raw[offset]))
	offset++

	vdata.Key = string(raw[offset : offset+klen])
	offset += klen

	vdata.TTL = int64(binary.BigEndian.Uint64(raw[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(raw[offset : offset+4])
	offset += 4
	vdata.Value = raw[offset : offset+int(vlen)]
	return vdata
}
