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

package table

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/pkg/errors"
)

const maxKeyLen = 256

type State uint8

const (
	ReadWriteState = State(iota + 1)
	ReadOnlyState
	RecycledState
)

var (
	ErrNotEnoughSpace = errors.New("not enough space")
	ErrHKeyNotFound   = errors.New("hkey not found")
)

type Stats struct {
	Allocated  uint32
	Inuse      uint32
	Garbage    uint32
	Length     int
	RecycledAt int64
}

type Table struct {
	offset     uint32
	allocated  uint32
	inuse      uint32
	garbage    uint32
	recycledAt int64
	state      State
	hkeys      map[uint64]uint32
	memory     []byte
}

func New(size uint32) *Table {
	t := &Table{
		hkeys:     make(map[uint64]uint32),
		allocated: size,
		state:     ReadWriteState,
	}
	//  From builtin.go:
	//
	//  The size specifies the length. The capacity of the slice is
	//	equal to its length. A second integer argument may be provided to
	//	specify a different capacity; it must be no smaller than the
	//	length. For example, make([]int, 0, 10) allocates an underlying array
	//	of size 10 and returns a slice of length 0 and capacity 10 that is
	//	backed by this underlying array.
	t.memory = make([]byte, size)
	return t
}

func (t *Table) SetState(s State) {
	t.state = s
}

func (t *Table) State() State {
	return t.state
}

func (t *Table) PutRaw(hkey uint64, value []byte) error {
	// Check empty space on allocated memory area.
	inuse := uint32(len(value))
	if inuse+t.offset >= t.allocated {
		return ErrNotEnoughSpace
	}
	t.hkeys[hkey] = t.offset
	copy(t.memory[t.offset:], value)
	t.inuse += inuse
	t.offset += inuse
	return nil
}

// In-memory layout for entry:
//
// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64) | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
func (t *Table) Put(hkey uint64, value storage.Entry) error {
	if len(value.Key()) >= maxKeyLen {
		return storage.ErrKeyTooLarge
	}

	// Check empty space on allocated memory area.

	// TTL + Timestamp + LastAccess + + value-Length + key-Length
	inuse := uint32(len(value.Key()) + len(value.Value()) + 29)
	if inuse+t.offset >= t.allocated {
		return ErrNotEnoughSpace
	}

	// If we already have the key, delete it.
	err := t.Delete(hkey)
	if errors.Is(err, ErrHKeyNotFound) {
		err = nil
	}
	if err != nil {
		return err
	}

	t.hkeys[hkey] = t.offset
	t.inuse += inuse

	// Set key length. It's 1 byte.
	klen := uint8(len(value.Key()))
	copy(t.memory[t.offset:], []byte{klen})
	t.offset++

	// Set the key.
	copy(t.memory[t.offset:], value.Key())
	t.offset += uint32(len(value.Key()))

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.TTL()))
	t.offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.Timestamp()))
	t.offset += 8

	// Set the last access. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(time.Now().UnixNano()))
	t.offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(t.memory[t.offset:], uint32(len(value.Value())))
	t.offset += 4

	// Set the value.
	copy(t.memory[t.offset:], value.Value())
	t.offset += uint32(len(value.Value()))
	return nil
}

func (t *Table) GetRaw(hkey uint64) ([]byte, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}
	start, end := offset, offset

	// In-memory structure:
	// 1                 | klen       | 8           | 8                  | 8                  | 4                    | vlen
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64)  | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := uint32(t.memory[end])
	end++       // One byte to keep key length
	end += klen // key length
	end += 8    // TTL
	end += 8    // Timestamp
	end += 8    // LastAccess

	vlen := binary.BigEndian.Uint32(t.memory[end : end+4])
	end += 4    // 4 bytes to keep value length
	end += vlen // value length

	// Create a copy of the requested data.
	rawval := make([]byte, end-start)
	copy(rawval, t.memory[start:end])
	return rawval, nil
}

func (t *Table) GetRawKey(hkey uint64) ([]byte, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}

	klen := uint32(t.memory[offset])
	offset++
	return t.memory[offset : offset+klen], nil
}

func (t *Table) GetKey(hkey uint64) (string, error) {
	raw, err := t.GetRawKey(hkey)
	if raw == nil {
		return "", err
	}
	return string(raw), err
}

func (t *Table) GetTTL(hkey uint64) (int64, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, ErrHKeyNotFound
	}

	klen := uint32(t.memory[offset])
	offset++
	offset += klen

	return int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])), nil
}

func (t *Table) GetLastAccess(hkey uint64) (int64, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, ErrHKeyNotFound
	}

	klen := uint32(t.memory[offset])
	offset++       // Key length
	offset += klen // Key's itself
	offset += 8    // TTL
	offset += 8    // Timestamp

	return int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])), nil
}

func (t *Table) Get(hkey uint64) (storage.Entry, error) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, ErrHKeyNotFound
	}

	e := &entry.Entry{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | TIMESTAMP(uint64) | LASTACCESS(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := uint32(t.memory[offset])
	offset++

	e.SetKey(string(t.memory[offset : offset+klen]))
	offset += klen

	e.SetTTL(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	offset += 8

	e.SetTimestamp(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	offset += 8

	e.SetLastAccess(int64(binary.BigEndian.Uint64(t.memory[offset : offset+8])))
	// Update the last access field
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(time.Now().UnixNano()))
	offset += 8

	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	offset += 4
	e.SetValue(t.memory[offset : offset+vlen])

	return e, nil
}

func (t *Table) Delete(hkey uint64) error {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous tables.
		return ErrHKeyNotFound
	}
	var garbage uint32

	// key, 1 byte for key size, klen for key's actual length.
	klen := uint32(t.memory[offset])
	offset += 1 + klen
	garbage += 1 + klen

	// TTL, skip it.
	offset += 8
	garbage += 8

	// Timestamp, skip it.
	offset += 8
	garbage += 8

	// LastAccess, skip it.
	offset += 8
	garbage += 8

	// value len and its header.
	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	garbage += 4 + vlen

	// Delete it from metadata
	delete(t.hkeys, hkey)

	t.garbage += garbage
	t.inuse -= garbage
	return nil
}

func (t *Table) UpdateTTL(hkey uint64, value storage.Entry) error {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return ErrHKeyNotFound
	}

	// key, 1 byte for key size, klen for key's actual length.
	klen := uint32(t.memory[offset])
	offset += 1 + klen

	// Set the new TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.TTL()))
	offset += 8

	// Set the new Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.Timestamp()))

	offset += 8

	// Update the last access field
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(time.Now().UnixNano()))

	return nil
}

func (t *Table) Check(hkey uint64) bool {
	_, ok := t.hkeys[hkey]
	return ok
}

func (t *Table) Stats() Stats {
	return Stats{
		Allocated:  t.allocated,
		Inuse:      t.inuse,
		Garbage:    t.garbage,
		Length:     len(t.hkeys),
		RecycledAt: t.recycledAt,
	}
}

func (t *Table) Range(f func(hkey uint64, e storage.Entry) bool) {
	for hkey := range t.hkeys {
		e, err := t.Get(hkey)
		if errors.Is(err, ErrHKeyNotFound) {
			panic(fmt.Errorf("hkey: %d found in index, but Get could not find it", hkey))
		}

		if !f(hkey, e) {
			break
		}
	}
}

func (t *Table) Reset() {
	if len(t.hkeys) != 0 {
		t.hkeys = make(map[uint64]uint32)
	}
	t.SetState(RecycledState)
	t.inuse = 0
	t.garbage = 0
	t.offset = 0
	t.recycledAt = time.Now().UnixNano()
}
