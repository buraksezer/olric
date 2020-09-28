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
	"fmt"

	"github.com/cespare/xxhash"
	"github.com/google/go-cmp/cmp"
)

// DiffReporter is a simple custom reporter that only records differences
// detected during comparison.
type DiffReporter struct {
	path    cmp.Path
	deleted []*DataSet
	added   []*DataSet
}

func (r *DiffReporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *DiffReporter) Report(rs cmp.Result) {
	if rs.Equal() {
		return
	}
	vx, vy := r.path.Last().Values()
	if vx.IsValid() {
		r.deleted = append(r.deleted, vx.Interface().(*DataSet))
	}
	if vy.IsValid() {
		r.added = append(r.added, vy.Interface().(*DataSet))
	}
}

func (r *DiffReporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *DiffReporter) String() string {
	for _, i := range r.deleted {
		fmt.Println("DELETED", i)
	}
	fmt.Println("-------------------")
	for _, i := range r.added {
		fmt.Println("ADDED", i)
	}
	return ""
}

type DataSet struct {
	HKey      uint64
	Timestamp uint64
}

func (d *DataSet) Add(key, timestamp uint64) {
	d.HKey = key
	d.Timestamp = timestamp
}

type Compare struct {
	one map[uint64]*DataSet
	two map[uint64]*DataSet
}

func prepare(ds []*DataSet) map[uint64]*DataSet {
	data := make(map[uint64]*DataSet)
	b := make([]byte, 16)
	for _, item := range ds {
		binary.LittleEndian.PutUint64(b[:8], item.HKey)
		binary.LittleEndian.PutUint64(b[8:], item.Timestamp)
		data[xxhash.Sum64(b)] = item
	}
	return data
}

func New(one, two []*DataSet) *Compare {
	return &Compare{
		one: prepare(one),
		two: prepare(two),
	}
}

func (c *Compare) Difference() {
	var r DiffReporter
	equal := cmp.Equal(c.one, c.two, cmp.Reporter(&r))
	if equal {
		return
	}
	fmt.Println(r.String())
}
