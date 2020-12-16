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

package partitions

import (
	"sync"
	"sync/atomic"

	"github.com/buraksezer/olric/internal/discovery"
)

// Partition is a basic, logical storage unit in Olric and stores DMaps in a sync.Map.
type Partition struct {
	sync.RWMutex

	id     uint64
	backup bool
	m      sync.Map
	owners atomic.Value
}

// owner returns partition owner. It's not thread-safe.
func (p *Partition) owner() discovery.Member {
	if p.backup {
		// programming error. it cannot occur at production!
		panic("cannot call this if backup is true")
	}
	owners := p.owners.Load().([]discovery.Member)
	if len(owners) == 0 {
		panic("owners list cannot be empty")
	}
	return owners[len(owners)-1]
}

// ownerCount returns the current owner count of a partition.
func (p *Partition) ownerCount() int {
	owners := p.owners.Load()
	if owners == nil {
		return 0
	}
	return len(owners.([]discovery.Member))
}

// loadOwners loads the partition owners from atomic.value and returns.
func (p *Partition) loadOwners() []discovery.Member {
	owners := p.owners.Load()
	if owners == nil {
		return []discovery.Member{}
	}
	return owners.([]discovery.Member)
}
