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

package dmap

import (
	"context"
	"errors"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
)

const DefaultScanCount = 10

// ErrEndOfQuery is the error returned by Range when no more data is available.
// Functions should return ErrEndOfQuery only to signal a graceful end of input.
var ErrEndOfQuery = errors.New("end of query")

// Iterator implements distributed query on DMaps.
type Iterator struct {
	mtx sync.Mutex

	pos      int
	page     []string
	dm       *DMap
	allKeys  map[string]struct{}
	finished map[uint64]struct{}
	cursors  map[uint64]uint64 // member id => cursor
	partID   uint64            // current partition id
	config   *scanConfig
	ctx      context.Context
	cancel   context.CancelFunc
}

func (dm *DMap) Scan(options ...ScanOption) (*Iterator, error) {
	var sc scanConfig
	for _, opt := range options {
		opt(&sc)
	}
	if sc.Count == 0 {
		sc.Count = DefaultScanCount
	}
	ctx, cancel := context.WithCancel(dm.s.ctx)
	return &Iterator{
		dm:       dm,
		config:   &sc,
		allKeys:  make(map[string]struct{}),
		finished: make(map[uint64]struct{}),
		cursors:  make(map[uint64]uint64),
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (i *Iterator) updateIterator(keys []string, cursor, ownerID uint64) {
	if cursor == 0 {
		i.finished[ownerID] = struct{}{}
	}
	i.cursors[ownerID] = cursor
	for _, key := range keys {
		if _, ok := i.allKeys[key]; !ok {
			i.page = append(i.page, key)
			i.allKeys[key] = struct{}{}
		}
	}
}

func (i *Iterator) scanOnOwners(owners []discovery.Member) error {
	for _, owner := range owners {
		if _, ok := i.finished[owner.ID]; ok {
			continue
		}
		if owner.CompareByID(i.dm.s.rt.This()) {
			keys, cursor, err := i.dm.scan(i.partID, i.cursors[owner.ID], i.config)
			if err != nil {
				return err
			}
			i.updateIterator(keys, cursor, owner.ID)
			continue
		}

		s := protocol.NewScan(i.partID, i.dm.name, i.cursors[owner.ID])
		if i.config.HasCount {
			s.SetCount(i.config.Count)
		}
		if i.config.HasMatch {
			s.SetMatch(s.Match)
		}
		scanCmd := s.Command(i.ctx)
		rc := i.dm.s.client.Get(owner.String())
		err := rc.Process(i.ctx, scanCmd)
		if err != nil {
			return err
		}
		keys, cursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		i.updateIterator(keys, cursor, owner.ID)
	}

	return nil
}

func (i *Iterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *Iterator) reset() {
	// Reset
	for memberID := range i.cursors {
		delete(i.cursors, memberID)
		delete(i.finished, memberID)
	}
	i.resetPage()
}

func (i *Iterator) next() bool {
	if len(i.page) != 0 {
		i.pos++
		if i.pos <= len(i.page) {
			return true
		}
	}

	i.resetPage()

	primaryOwners := i.dm.s.primary.PartitionOwnersByID(i.partID)
	i.config.replica = false
	if err := i.scanOnOwners(primaryOwners); err != nil {
		return false
	}

	replicaOwners := i.dm.s.backup.PartitionOwnersByID(i.partID)
	i.config.replica = true
	if err := i.scanOnOwners(replicaOwners); err != nil {
		return false
	}

	if len(i.page) == 0 {
		i.partID++
		if i.dm.s.config.PartitionCount <= i.partID {
			return false
		}
		i.reset()
		return i.next()
	}
	i.pos = 1
	return true
}

func (i *Iterator) Next() bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-i.ctx.Done():
		return false
	default:
	}

	return i.next()
}

func (i *Iterator) Key() string {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var key string
	if i.pos > 0 && i.pos <= len(i.page) {
		key = i.page[i.pos-1]
	}
	return key
}

func (i *Iterator) Close() {
	select {
	case <-i.ctx.Done():
		return
	default:
	}
	i.cancel()
}
