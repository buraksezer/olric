// Copyright 2018-2022 Burak Sezer
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

package olric

import (
	"context"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
)

// EmbeddedIterator implements distributed query on DMaps.
type EmbeddedIterator struct {
	mtx sync.Mutex

	client         *EmbeddedClient
	pos            int
	page           []string
	dm             *dmap.DMap
	allKeys        map[string]struct{}
	cursors        map[uint64]map[string]uint64 // member id => cursor
	replicaCursors map[uint64]map[string]uint64 // member id => cursor
	partID         uint64                       // current partition id
	config         *dmap.ScanConfig
	ctx            context.Context
	cancel         context.CancelFunc
}

func (i *EmbeddedIterator) updateIterator(keys []string, cursor uint64, owner string) {
	if _, ok := i.cursors[i.partID]; !ok {
		i.cursors[i.partID] = make(map[string]uint64)
	}
	if _, ok := i.replicaCursors[i.partID]; !ok {
		i.replicaCursors[i.partID] = make(map[string]uint64)
	}

	if i.config.Replica {
		i.replicaCursors[i.partID][owner] = cursor
	} else {
		i.cursors[i.partID][owner] = cursor
	}
	for _, key := range keys {
		if _, ok := i.allKeys[key]; !ok {
			i.page = append(i.page, key)
			i.allKeys[key] = struct{}{}
		}
	}
}

func (i *EmbeddedIterator) scanOnOwners(owners []discovery.Member) error {
	for _, owner := range owners {
		var cursor = i.cursors[i.partID][owner.String()]
		if i.config.Replica {
			cursor = i.replicaCursors[i.partID][owner.String()]
		}

		if owner.CompareByID(i.client.db.rt.This()) {
			keys, cursor, err := i.dm.Scan(i.partID, cursor, i.config)
			if err != nil {
				return err
			}
			i.updateIterator(keys, cursor, owner.String())
			continue
		}

		s := protocol.NewScan(i.partID, i.dm.Name(), cursor)
		if i.config.HasCount {
			s.SetCount(i.config.Count)
		}
		if i.config.HasMatch {
			s.SetMatch(i.config.Match)
		}
		if i.config.Replica {
			s.SetReplica()
		}

		scanCmd := s.Command(i.ctx)
		// Fetch a redis rc for the given owner.
		rc := i.client.db.client.Get(owner.String())
		err := rc.Process(i.ctx, scanCmd)
		if err != nil {
			return err
		}

		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		i.updateIterator(keys, newCursor, owner.String())
	}

	return nil
}

func (i *EmbeddedIterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *EmbeddedIterator) fetchData() error {
	primaryOwners := i.client.db.primary.PartitionOwnersByID(i.partID)
	i.config.Replica = false
	if err := i.scanOnOwners(primaryOwners); err != nil {
		return err
	}

	replicaOwners := i.client.db.backup.PartitionOwnersByID(i.partID)
	i.config.Replica = true
	if err := i.scanOnOwners(replicaOwners); err != nil {
		return err
	}

	return nil
}

func (i *EmbeddedIterator) next() bool {
	if len(i.page) != 0 {
		i.pos++
		if i.pos <= len(i.page) {
			return true
		}
	}

	i.resetPage()

	if err := i.fetchData(); err != nil {
		// TODO: log these errors!
		return false
	}

	if len(i.page) == 0 {
		i.partID++
		if i.partID >= i.client.db.config.PartitionCount {
			return false
		}
		i.resetPage()
		return i.next()
	}
	i.pos = 1
	return true
}

func (i *EmbeddedIterator) Next() bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-i.ctx.Done():
		return false
	default:
	}

	return i.next()
}

func (i *EmbeddedIterator) Key() string {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var key string
	if i.pos > 0 && i.pos <= len(i.page) {
		key = i.page[i.pos-1]
	}
	return key
}

func (i *EmbeddedIterator) Close() {
	select {
	case <-i.ctx.Done():
		return
	default:
	}
	i.cancel()
}
