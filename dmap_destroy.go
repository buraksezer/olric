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

package olric

import (
	"runtime"

	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (db *Olric) destroyDMap(name string) error {
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	var g errgroup.Group
	for _, item := range db.discovery.GetMembers() {
		addr := item.String()
		g.Go(func() error {
			if err := sem.Acquire(db.ctx, 1); err != nil {
				db.log.V(3).
					Printf("[ERROR] Failed to acquire semaphore to call Destroy command on %s for %s: %v",
						addr, name, err)
				return err
			}
			defer sem.Release(1)

			req := protocol.NewDMapMessage(protocol.OpDestroyDMap)
			req.SetDMap(name)
			db.log.V(6).Printf("[DEBUG] Calling Destroy command on %s for %s", addr, name)
			_, err := db.requestTo(addr, req)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to destroy DMap: %s on %s", name, addr)
			}
			return err
		})
	}
	return g.Wait()
}

// Destroy flushes the given dmap on the cluster. You should know that there
// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
// concurrently on the cluster, Put/PutEx calls may set new values to the dmap.
func (dm *DMap) Destroy() error {
	return dm.db.destroyDMap(dm.name)
}

func (db *Olric) exDestroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	err := db.destroyDMap(req.DMap())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) destroyDMapOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	// This is very similar with rm -rf. Destroys given dmap on the cluster
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		// Delete primary copies
		part := db.partitions[partID]
		part.m.Delete(req.DMap())
		// Delete from Backups
		if db.config.ReplicaCount != 0 {
			bpart := db.backups[partID]
			bpart.m.Delete(req.DMap())
		}
	}
	w.SetStatus(protocol.StatusOK)
}
