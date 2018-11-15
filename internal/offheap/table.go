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
	"encoding/binary"
	"syscall"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const maxKeyLen = 256

var (
	errNotEnoughSpace = errors.New("not enough space")

	// ErrKeyTooLarge is an error that indicates the given key is large than the determined key size.
	// The current maximum key length is 256.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrKeyNotFound is an error that indicates that the requested key could not be found in the DB.
	ErrKeyNotFound = errors.New("key not found")
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

func newTable(size int) (*table, error) {
	if size < minimumSize {
		size = minimumSize
	}
	t := &table{
		hkeys:     make(map[uint64]int),
		allocated: size,
	}
	err := t.malloc(size)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *table) close() error {
	return unix.Munmap(t.memory)
}

// malloc allocates memory with mmap syscall on RAM.
func (t *table) malloc(size int) error {
	flags := syscall.MAP_ANON | syscall.MAP_PRIVATE
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	array, err := unix.Mmap(-1, 0, size, prot, flags)
	if err != nil {
		return err
	}
	t.memory = array
	return nil
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
// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
func (t *table) put(hkey uint64, value *VData) error {
	if len(value.Key) >= maxKeyLen {
		return ErrKeyTooLarge
	}

	// Check empty space on allocated memory area.
	inuse := len(value.Key) + len(value.Value) + 13
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
	klen := uint8(len(value.Key))
	copy(t.memory[t.offset:], []byte{klen})
	t.offset++

	// Set the key.
	copy(t.memory[t.offset:], value.Key)
	t.offset += len(value.Key)

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(t.memory[t.offset:], uint64(value.TTL))
	t.offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(t.memory[t.offset:], uint32(len(value.Value)))
	t.offset += 4

	// Set the value.
	copy(t.memory[t.offset:], value.Value)
	t.offset += len(value.Value)
	return nil
}

func (t *table) getRaw(hkey uint64) ([]byte, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}
	start, end := offset, offset

	// In-memory structure:
	// 1                 | klen       | 8           | 4                    | vlen
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(uint8(t.memory[end]))
	end++       // One byte to keep key length
	end += klen // Key length
	end += 8    // For bytes for TTL

	vlen := binary.BigEndian.Uint32(t.memory[end : end+4])
	end += 4         // 4 bytes to keep value length
	end += int(vlen) // Value length

	// Create a copy of the requested data.
	rawval := make([]byte, (end-start)+1)
	copy(rawval, t.memory[start:end])
	return rawval, false
}

func (t *table) get(hkey uint64) (*VData, bool) {
	offset, ok := t.hkeys[hkey]
	if !ok {
		return nil, true
	}

	vdata := &VData{}
	// In-memory structure:
	//
	// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)
	klen := int(uint8(t.memory[offset]))
	offset++

	vdata.Key = string(t.memory[offset : offset+klen])
	offset += klen

	vdata.TTL = int64(binary.BigEndian.Uint64(t.memory[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	offset += 4
	vdata.Value = t.memory[offset : offset+int(vlen)]
	return vdata, false
}

func (t *table) delete(hkey uint64) bool {
	offset, ok := t.hkeys[hkey]
	if !ok {
		// Try the previous table.
		return true
	}
	var garbage int

	// Key, 1 byte for key size, klen for key's actual length.
	klen := int(uint8(t.memory[offset]))
	offset += 1 + klen
	garbage += 1 + klen

	// TTL, skip it.
	offset += 8
	garbage += 8

	// Value len and its header.
	vlen := binary.BigEndian.Uint32(t.memory[offset : offset+4])
	garbage += 4 + int(vlen)

	// Delete it from metadata
	delete(t.hkeys, hkey)

	t.garbage += garbage
	t.inuse -= garbage
	return false
}
