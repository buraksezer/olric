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

package routing_table

import (
	"fmt"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
)

type members struct {
	sync.RWMutex
	m map[uint64]discovery.Member
}

func newMembers() *members {
	return &members{
		m: map[uint64]discovery.Member{},
	}
}

func (m *members) Add(member discovery.Member) {
	m.m[member.ID] = member
}

func (m *members) Get(id uint64) (discovery.Member, error) {
	member, ok := m.m[id]
	if !ok {
		return discovery.Member{}, fmt.Errorf("member not found with id: %d", id)
	}
	return member, nil
}

func (m *members) Delete(id uint64) {
	delete(m.m, id)
}

func (m *members) DeleteByName(other discovery.Member) {
	for id, member := range m.m {
		if member.CompareByName(other) {
			delete(m.m, id)
		}
	}
}

func (m *members) Length() int {
	return len(m.m)
}

func (m *members) Range(f func(id uint64, member discovery.Member)) {
	for id, member := range m.m {
		f(id, member)
	}
}
