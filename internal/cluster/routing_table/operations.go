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

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/cespare/xxhash"
	"github.com/vmihailenco/msgpack"
)

func response(w protocol.EncodeDecoder, statusCode protocol.StatusCode, value []byte) {
	w.SetStatus(statusCode)
	w.SetValue(value)
}

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
		response(w, protocol.StatusInternalServerError, []byte(err.Error()))
		return
	}
	response(w, protocol.StatusOK, value)
}

func (r *RoutingTable) verifyRoutingTable(id uint64, table map[uint64]*route) error {
	// Check the coordinator
	coordinator, err := r.discovery.FindMemberByID(id)
	if err != nil {
		return err
	}

	myCoordinator := r.discovery.GetCoordinator()
	if !coordinator.CompareByID(myCoordinator) {
		return fmt.Errorf("unrecognized cluster coordinator: %s: %s", coordinator, myCoordinator)
	}

	// Compare partition counts to catch a possible inconsistencies in configuration
	if r.config.PartitionCount != uint64(len(table)) {
		return fmt.Errorf("invalid partition count: %d", len(table))
	}
	return nil
}

func (r *RoutingTable) UpdateRoutingOperation(w, rq protocol.EncodeDecoder) {
	r.updateRoutingMtx.Lock()
	defer r.updateRoutingMtx.Unlock()

	req := rq.(*protocol.SystemMessage)
	table := make(map[uint64]*route)
	err := msgpack.Unmarshal(req.Value(), &table)
	if err != nil {
		response(w, protocol.StatusInternalServerError, []byte(err.Error()))
		return
	}

	coordinatorID := req.Extra().(protocol.UpdateRoutingExtra).CoordinatorID

	// Log this event
	coordinator, err := r.discovery.FindMemberByID(coordinatorID)
	if err != nil {
		response(w, protocol.StatusInternalServerError, []byte(err.Error()))
		return
	}
	r.log.V(3).Printf("[INFO] Routing table has been pushed by %s", coordinator)

	if err = r.verifyRoutingTable(coordinatorID, table); err != nil {
		response(w, protocol.StatusInternalServerError, []byte(err.Error()))
		return
	}

	// owners(atomic.value) is guarded by routingUpdateMtx against parallel writers.
	// Calculate routing signature. This is useful to control rebalancing tasks.
	r.setSignature(xxhash.Sum64(req.Value()))
	for partID, data := range table {
		// Set partition(primary copies) owners
		part := r.primary.PartitionById(partID)
		part.SetOwners(data.Owners)

		// Set backup owners
		bpart := r.backup.PartitionById(partID)
		bpart.SetOwners(data.Backups)
	}

	// Used by the LRU implementation.
	r.setOwnedPartitionCount()

	// Bootstrapped by the coordinator.
	r.markBootstrapped()

	// Collect report
	value, err := r.prepareLeftOverDataReport()
	if err != nil {
		response(w, protocol.StatusInternalServerError, []byte(err.Error()))
		return
	}

	// Call rebalancer to rebalance partitions
	r.wg.Add(1)
	go r.runCallbacks()
	response(w, protocol.StatusOK, value)
}
