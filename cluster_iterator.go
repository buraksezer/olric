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
	"fmt"
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
	allKeys        map[string]struct{}
	cursors        map[string]uint64 // member id => cursor
	replicaCursors map[string]uint64 // member id => cursor
	partID         uint64            // current partition id
	routingTable   RoutingTable
	partitionCount uint64
	config         *dmap.ScanConfig
	wg             sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
}

func (i *ClusterIterator) updateIterator(keys []string, cursor uint64, owner string) {
	if i.config.Replica {
		i.replicaCursors[owner] = cursor
	} else {
		i.cursors[owner] = cursor
	}
	for _, key := range keys {
		if _, ok := i.allKeys[key]; !ok {
			i.page = append(i.page, key)
			i.allKeys[key] = struct{}{}
		}
	}
}

func (i *ClusterIterator) scanOnOwners(owners []string) error {
	for _, owner := range owners {
		var cursor uint64 = i.cursors[owner]
		if i.config.Replica {
			cursor = i.replicaCursors[owner]
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
			return err
		}

		keys, cursor, err := scanCmd.Result()
		if err != nil {
			return err
		}
		i.updateIterator(keys, cursor, owner)
	}

	return nil
}

func (i *ClusterIterator) resetPage() {
	if len(i.page) != 0 {
		i.page = []string{}
	}
	i.pos = 0
}

func (i *ClusterIterator) reset() {
	// Reset
	for memberID := range i.cursors {
		delete(i.cursors, memberID)
		delete(i.replicaCursors, memberID)
	}
	i.resetPage()
}

func (i *ClusterIterator) fetchData() error {
	i.rtMtx.Lock()
	defer i.rtMtx.Unlock()

	route, ok := i.routingTable[i.partID]
	if !ok {
		return fmt.Errorf("partID: %d could not be found in the routing table", i.partID)
	}

	i.config.Replica = false
	if err := i.scanOnOwners(route.PrimaryOwners); err != nil {
		return err
	}

	i.config.Replica = true
	if err := i.scanOnOwners(route.ReplicaOwners); err != nil {
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

	if err := i.fetchData(); err != nil {
		i.logger.Printf("[ERROR] Failed to fetch data: %s", err)
		return false
	}

	if len(i.page) == 0 {
		i.partID++
		if i.partID >= i.partitionCount {
			return false
		}
		i.reset()
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
