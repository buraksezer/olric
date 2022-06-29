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
	"log"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/protocol"
)

// ClusterIterator implements distributed query on DMaps.
type ClusterIterator struct {
	mtx   sync.Mutex // protects pos and page
	rtMtx sync.Mutex // protects routingTable and partitionCount

	logger         *log.Logger
	dm             *ClusterDMap
	clusterClient  *ClusterClient
	pos            int
	page           []string
	route          *Route
	allKeys        map[string]struct{}
	cursors        map[uint64]map[string]uint64 // member id => cursor
	replicaCursors map[uint64]map[string]uint64 // member id => cursor
	partID         uint64                       // current partition id
	routingTable   RoutingTable
	partitionCount uint64
	config         *dmap.ScanConfig
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func (i *ClusterIterator) updateIterator(keys []string, cursor uint64, owner string) {
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

func (i *ClusterIterator) scanOnOwners(owners []string) ([]string, error) {
	data := make(map[string]bool)
	for _, owner := range owners {
		var cursor = i.cursors[i.partID][owner]
		if i.config.Replica {
			cursor = i.replicaCursors[i.partID][owner]
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
		// Fetch a Redis client for the given owner.
		rc := i.clusterClient.client.Get(owner)
		err := rc.Process(i.ctx, scanCmd)
		if err != nil {
			return nil, err
		}

		keys, newCursor, err := scanCmd.Result()
		if err != nil {
			return nil, err
		}
		i.updateIterator(keys, newCursor, owner)
		if newCursor == 0 {
			data[owner] = true
		}
	}

	var newowners []string
	for _, owner := range owners {
		if !data[owner] {
			newowners = append(newowners, owner)
		}
	}

	return newowners, nil
}

func (i *ClusterIterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *ClusterIterator) fetchData() error {
	var err error
	i.config.Replica = false
	i.route.PrimaryOwners, err = i.scanOnOwners(i.route.PrimaryOwners)
	if err != nil {
		return err
	}

	i.config.Replica = true
	i.route.ReplicaOwners, err = i.scanOnOwners(i.route.ReplicaOwners)
	if err != nil {
		return err
	}

	return nil
}

func (i *ClusterIterator) next() bool {
	if len(i.page) != 0 {
		i.pos++
		if i.pos <= len(i.page) {
			return true
		}
	}

	i.resetPage()

	for {
		if err := i.fetchData(); err != nil {
			i.logger.Printf("[ERROR] Failed to fetch data: %s", err)
			return false
		}
		if len(i.page) != 0 {
			break
		}
		if len(i.route.PrimaryOwners) == 0 && len(i.route.ReplicaOwners) == 0 {
			break
		}
	}

	if len(i.page) == 0 && len(i.route.PrimaryOwners) == 0 && len(i.route.ReplicaOwners) == 0 {
		i.partID++
		if i.partID >= i.partitionCount {
			return false
		}
		i.resetPage()

		i.rtMtx.Lock()
		route, ok := i.routingTable[i.partID]
		i.rtMtx.Unlock()
		if !ok {
			panic("partID: could not be found in the routing table")
		}
		i.route = &route

		return i.next()
	}
	i.pos = 1
	return true
}

func (i *ClusterIterator) Next() bool {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	select {
	case <-i.ctx.Done():
		return false
	default:
	}

	i.rtMtx.Lock()
	if i.route == nil {
		route, ok := i.routingTable[i.partID]
		if !ok {
			panic("partID: could not be found in the routing table")
		}
		i.route = &route
	}
	i.rtMtx.Unlock()

	return i.next()
}

func (i *ClusterIterator) Key() string {
	i.mtx.Lock()
	defer i.mtx.Unlock()

	var key string
	if i.pos > 0 && i.pos <= len(i.page) {
		key = i.page[i.pos-1]
	}
	return key
}

func (i *ClusterIterator) fetchRoutingTablePeriodically() {
	defer i.wg.Done()

	for {
		select {
		case <-i.ctx.Done():
			return
		case <-time.After(time.Second):
			if err := i.fetchRoutingTable(); err != nil {
				i.logger.Printf("[ERROR] Failed to fetch the latest routing table: %s", err)
			}
		}
	}
}

func (i *ClusterIterator) fetchRoutingTable() error {
	rt, err := i.clusterClient.RoutingTable(i.ctx)
	if err != nil {
		return err
	}

	i.rtMtx.Lock()
	defer i.rtMtx.Unlock()

	// Partition count is a constant, actually. It has to be greater than zero.
	i.partitionCount = uint64(len(rt))
	i.routingTable = rt
	return nil
}

func (i *ClusterIterator) Close() {
	select {
	case <-i.ctx.Done():
		return
	default:
	}

	i.cancel()

	// await for routing table updater
	i.wg.Wait()
}
