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
	"sync/atomic"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
)

func (r *RoutingTable) KeyCountOnPartOperation(w, rq protocol.EncodeDecoder) {
	req := rq.(*protocol.SystemMessage)
	partID := req.Extra().(protocol.LengthOfPartExtra).PartID
	isBackup := req.Extra().(protocol.LengthOfPartExtra).Backup

	var part *partitions.Partition
	if isBackup {
		part = r.backup.PartitionById(partID)
	} else {
		part = r.primary.PartitionById(partID)
	}

	value, err := msgpack.Marshal(part.Length())
	if err != nil {
		w.SetStatus(protocol.StatusInternalServerError)
		w.SetValue([]byte(err.Error()))
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (r *RoutingTable) updateRoutingOperation(w, rq protocol.EncodeDecoder) {
	r.updateRoutingMtx.Lock()
	defer r.updateRoutingMtx.Unlock()

	req := rq.(*protocol.SystemMessage)
	table := make(map[uint64]*route)
	err := msgpack.Unmarshal(req.Value(), &table)
	if err != nil {
		w.SetStatus(protocol.StatusInternalServerError)
		w.SetValue([]byte(err.Error()))
		return
	}

	coordinatorID := req.Extra().(protocol.UpdateRoutingExtra).CoordinatorID
	coordinator, err := db.checkAndGetCoordinator(coordinatorID)
	if err != nil {
		r.log.V(2).Printf("[ERROR] Routing table cannot be updated: %v", err)
		db.errorResponse(w, err)
		return
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if r.config.PartitionCount != uint64(len(table)) {
		r.log.V(2).Printf("[ERROR] Routing table cannot be updated. "+
			"Expected partition count is %d, got: %d", r.config.PartitionCount, uint64(len(table)))
		db.errorResponse(w, ErrInvalidArgument)
		return
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control rebalancing tasks.
	r.SetSignature(r.hasher.Sum64(req.Value()))
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := r.primary.PartitionById(partID)
		part.SetOwners(data.Owners)

		// Set backup owners
		bpart := r.backup.PartitionById(partID)
		bpart.SetOwners(data.Backups)
	}

	db.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	atomic.StoreInt32(&db.bootstrapped, 1)
	// Collect report
	data, err := r.prepareOwnershipReport()
	if err != nil {
		db.errorResponse(w, ErrInvalidArgument)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(data)

	// Call rebalancer to rebalance partitions
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// TODO: Add rebalancer
		// db.rebalancer()

		// TODO: This can be moved
		// Clean stale dmaps
		// db.deleteStaleDMaps()
	}()
	r.log.V(3).Printf("[INFO] Routing table has been pushed by %s", coordinator)
}
