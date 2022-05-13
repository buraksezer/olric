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

package kvstore

import (
	"encoding/binary"

	"github.com/buraksezer/olric/pkg/storage"
	"github.com/pkg/errors"
)

const (
	maxKeyLen   = 256
	metadataLen = 21 // KEY-LENGTH(uint8) + TTL(uint64) + Timestamp(uint64) + VALUE-LENGTH(uint32)
)

var (
	errNotEnoughSpace = errors.New("not enough space")
)

type table struct {
	hkeys  map[uint64]int
	memory []byte
	offset int

	// In bytes
	allocated int
	inuse     int
	garbage   int
}

func newTable(size int) *table {
	t := &table{
		hkeys:     make(map[uint64]int),
		allocated: size,
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

func (t *table) putRaw(hkey uint64, value []byte) error {
	// Check empty space on allocated memory area.
	inuse := len(value)
	if inuse+t.offset >= t.allocated {
		return errNotEnoughSpace
	}
	t.hkeys[hkey] = t.offset
	copy(t.memory[t.offset:], value)
	t.inuse += inuse
	t.offset += inuse
	return nil
}

// In-memory layout for entry:
//
// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | | Timestamp(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
func (t *table) put(hkey uint64, value storage.Entry) error {
	if len(value.Key()) >= maxKeyLen {
		return storage.ErrKeyTooLarge
	}

	// Check empty space on allocated memory area.
	inuse := len(value.Key()) + len(value.Value()) + metadataLen // TTL + Timestamp + value-Length + key-Length
	if inuse+t.offset >= t.allocated {
		return errNotEnoughSpace
	}

	// If we already have the key, delete it.
	if _, ok := t.hkeys[hkey]; ok {
		t.delete(hkey)
	}

	t.hkeys[hkey] = t.offset
	t.inuse += inuse

	// Set key length. It's 1 byte.
	klen := uint8(len(value.Key()))
	copy(t.memory[t.offset:], []byte{klen})
	t.offset++

	// Set the key.
	copy(t.memory[t.offset:], value.Key())
	t.offset += len(value.Key())

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.TTL()))
	t.offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.Timestamp()))
	t.offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(t.memory[t.offset:], uint32(len(value.Value())))
	t.offset += 4

	// Set the value.
	copy(t.memory[t.offset:], value.Value())
	t.offset += len(value.Value())
	return nil
}

func (t *table) getRaw(hkey uint64) ([]byte, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}
	start, end := offset, offset

	// In-memory structure:
	// 1                 | klen       | 8           | 8                  | 4                    | vlen
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | Timestamp(uint64)  | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(t.memory[end])
	end++       // One byte to keep key length
	end += klen // key length
	end += 8    // For bytes for TTL
	end += 8    // For bytes for Timestamp

	vlen := binary.BigEndian.Uint32(t.memory[end : end+4])
	end += 4         // 4 bytes to keep value length
	end += int(vlen) // value length

	// Create a copy of the requested data.
	rawval := make([]byte, (end-start)+1)
	copy(rawval, t.memory[start:end])
	return rawval, false
}

func (t *table) getRawKey(hkey uint64) ([]byte, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}

	klen := int(t.memory[offset])
	offset++
	return t.memory[offset : offset+klen], false
}

func (t *table) getKey(hkey uint64) (string, bool) {
	raw, prev := t.getRawKey(hkey)
	if raw == nil {
		return "", prev
	}
	return string(raw), prev
}

func (t *table) getTTL(hkey uint64) (int64, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return 0, true
	}

	klen := int(t.memory[offset])
	offset++
	offset += klen
	ttl := int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	return ttl, false
}

func (t *table) get(hkey uint64) (storage.Entry, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}

	entry := &Entry{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | Timestamp(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(uint8(t.memory[offset]))
	offset++

	entry.key = string(t.memory[offset : offset+klen])
	offset += klen

	entry.ttl = int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	offset += 8

	entry.timestamp = int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	offset += 4
	entry.value = t.memory[offset : offset+int(vlen)]
	return entry, false
}

func (t *table) delete(hkey uint64) bool {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous table.
		return true
	}
	var garbage int

	// key, 1 byte for key size, klen for key's actual length.
	klen := int(uint8(t.memory[offset]))
	offset += 1 + klen
	garbage += 1 + klen

	// TTL, skip it.
	offset += 8
	garbage += 8

	// Timestamp, skip it.
	offset += 8
	garbage += 8

	// value len and its header.
	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	garbage += 4 + int(vlen)

	// Delete it from metadata
	delete(t.hkeys, hkey)

	t.garbage += garbage
	t.inuse -= garbage
	return false
}

func (t *table) updateTTL(hkey uint64, value storage.Entry) bool {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous table.
		return true
	}

	// key, 1 byte for key size, klen for key's actual length.
	klen := int(uint8(t.memory[offset]))
	offset += 1 + klen

	// Set the new TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.TTL()))
	offset += 8

	// Set the new Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[offset:], uint64(value.Timestamp()))
	return false
}
