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

package compare

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/google/go-cmp/cmp"
	"github.com/vmihailenco/msgpack"
)

var ErrDatasetsEqual = errors.New("datasets are equal")

// Reporter is a simple custom reporter that only records differences
// detected during comparison.
type Reporter struct {
	path    cmp.Path
	deleted []*KVItem
	added   []*KVItem
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) Report(rs cmp.Result) {
	if rs.Equal() {
		return
	}
	vx, vy := r.path.Last().Values()
	if vx.IsValid() {
		r.deleted = append(r.deleted, vx.Interface().(*KVItem))
	}
	if vy.IsValid() {
		r.added = append(r.added, vy.Interface().(*KVItem))
	}
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

type KVItem struct {
	HKey      uint64
	Timestamp uint64
}

type Dataset struct {
	buf []byte
	m   map[uint64]*KVItem

	mu sync.RWMutex
}

func NewDataSet() *Dataset {
	return &Dataset{
		m:   make(map[uint64]*KVItem),
		buf: make([]byte, 16),
	}
}

func (d *Dataset) KVItems() map[uint64]*KVItem {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.m
}

func (d *Dataset) Add(hkey, timestamp uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	binary.LittleEndian.PutUint64(d.buf[:8], hkey)
	binary.LittleEndian.PutUint64(d.buf[8:], timestamp)
	d.m[xxhash.Sum64(d.buf)] = &KVItem{
		HKey:      hkey,
		Timestamp: timestamp,
	}
}

func (d *Dataset) Export() ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return msgpack.Marshal(d.m)
}

func Import(data []byte) (*Dataset, error) {
	d := NewDataSet()

	err := msgpack.Unmarshal(data, &d.m)
	if err != nil {
		return nil, err
	}
	return d, nil
}

func Cmp(one, two *Dataset) ([]*KVItem, []*KVItem, error) {
	var r Reporter
	equal := cmp.Equal(one.KVItems(), two.KVItems(), cmp.Reporter(&r))
	if equal {
		return nil, nil, ErrDatasetsEqual
	}
	return r.added, r.deleted, nil
}
