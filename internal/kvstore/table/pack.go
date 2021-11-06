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

import "github.com/vmihailenco/msgpack"

type Pack struct {
	Offset     uint32
	Allocated  uint32
	Inuse      uint32
	Garbage    uint32
	RecycledAt int64
	State      State
	HKeys      map[uint64]uint32
	Memory     []byte
}

func Encode(t *Table) ([]byte, error) {
	p := Pack{
		Offset:     t.offset,
		Allocated:  t.allocated,
		Inuse:      t.inuse,
		Garbage:    t.garbage,
		RecycledAt: t.recycledAt,
		State:      t.state,
		HKeys:      t.hkeys,
	}
	p.Memory = make([]byte, t.offset)
	copy(p.Memory, t.memory[:t.offset])

	return msgpack.Marshal(p)
}

func Decode(data []byte) (*Table, error) {
	p := &Pack{}

	err := msgpack.Unmarshal(data, p)
	if err != nil {
		return nil, err
	}

	t := New(p.Allocated)
	t.offset = p.Offset
	t.inuse = p.Inuse
	t.garbage = p.Garbage
	t.recycledAt = p.RecycledAt
	t.state = p.State
	t.hkeys = p.HKeys

	copy(t.memory[:t.offset], p.Memory)

	return t, nil
}
