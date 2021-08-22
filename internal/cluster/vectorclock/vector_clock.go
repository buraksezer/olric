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

package vectorclock

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
)

type VectorClock struct {
	mtx sync.RWMutex

	m map[uint64]uint64
}

func New() *VectorClock {
	return &VectorClock{
		m: make(map[uint64]uint64),
	}
}

func (v *VectorClock) Set(id, timestamp uint64) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	v.m[id] = timestamp
}

func (v *VectorClock) Tick(id uint64) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	v.m[id]++
}

func (v *VectorClock) Get(id uint64) (uint64, bool) {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	timestamp, ok := v.m[id]
	return timestamp, ok
}

func (v *VectorClock) Encode() []byte {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	var i int
	data := make([]byte, 16*len(v.m))
	for id, timestamp := range v.m {
		binary.BigEndian.PutUint64(data[i:], id)
		i += 8

		binary.BigEndian.PutUint64(data[i:], timestamp)
		i += 8
	}

	return data
}

func (v *VectorClock) Decode(data []byte) {
	v.mtx.Lock()
	defer v.mtx.Unlock()

	var length = len(data) / 16

	var start int
	var end = 16
	for i := 0; i < length; i++ {
		raw := data[start:end]
		id := binary.BigEndian.Uint64(raw[0:8])
		timestamp := binary.BigEndian.Uint64(raw[8:16])

		v.m[id] = timestamp
		start += 16
		end += 16
	}
}

func (v *VectorClock) String() string {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	var result strings.Builder
	result.WriteString("{\n")
	for id, timestamp := range v.m {
		result.WriteString(fmt.Sprintf("\t<id: %d, timestamp: %d>", id, timestamp))
		result.WriteString("\n")
	}
	result.WriteString("}")
	return result.String()
}

func (v *VectorClock) Range(f func(id, timestamp uint64)) {
	v.mtx.RLock()
	defer v.mtx.RUnlock()

	for id, timestamp := range v.m {
		f(id, timestamp)
	}
}
