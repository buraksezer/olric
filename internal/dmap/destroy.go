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

package dmap

import (
	"runtime"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (dm *DMap) destroy(name string) error {
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)

	var g errgroup.Group

	// Don't block routing table to destroy a DMap on the cluster.
	// Just get a copy of members and run Destroy.
	var members []discovery.Member
	m := dm.s.rt.Members()
	m.RLock()
	m.Range(func(_ uint64, member discovery.Member) bool {
		members = append(members, member)
		return true
	})
	m.RUnlock()

	for _, item := range members {
		addr := item.String()
		g.Go(func() error {
			if err := sem.Acquire(dm.s.ctx, 1); err != nil {
				dm.s.log.V(3).
					Printf("[ERROR] Failed to acquire semaphore to call Destroy command on %s for %s: %v",
						addr, name, err)
				return err
			}
			defer sem.Release(1)

			req := protocol.NewDMapMessage(protocol.OpDestroyDMap)
			req.SetDMap(name)
			dm.s.log.V(6).Printf("[DEBUG] Calling Destroy command on %s for %s", addr, name)
			_, err := dm.s.client.RequestTo2(addr, req)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to destroy DMap: %s on %s", name, addr)
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
	return dm.destroy(dm.name)
}
