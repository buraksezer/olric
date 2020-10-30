// Copyright 2018-2020 Burak Sezer
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

package storage

import "encoding/binary"

// In-memory layout for an entry:
//
// KEY-LENGTH(uint8) | KEY(bytes) | TTL(uint64) | | Timestamp(uint64) | VALUE-LENGTH(uint32) | VALUE(bytes)

// Entry represents a value with its metadata.
type Entry struct {
	Key       string
	TTL       int64
	Timestamp int64
	Value     []byte
}

func NewEntry() *Entry {
	return &Entry{}
}

func (e *Entry) Encode() []byte {
	var offset int

	klen := uint8(len(e.Key))
	vlen := len(e.Value)
	length := 21 + len(e.Key) + vlen

	buf := make([]byte, length)

	// Set key length. It's 1 byte.
	copy(buf[offset:], []byte{klen})
	offset++

	// Set the key.
	copy(buf[offset:], e.Key)
	offset += len(e.Key)

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.TTL))
	offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.Timestamp))
	offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Value)))
	offset += 4

	// Set the value.
	copy(buf[offset:], e.Value)
	offset += len(e.Value)
	return buf
}

func (e *Entry) Decode(buf []byte) {
	var offset int

	klen := int(uint8(buf[offset]))
	offset++

	e.Key = string(buf[offset : offset+klen])
	offset += klen

	e.TTL = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8

	e.Timestamp = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	e.Value = buf[offset : offset+int(vlen)]
}
