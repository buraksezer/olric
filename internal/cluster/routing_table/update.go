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
	"runtime"
	"sync"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type leftOverDataReport struct {
	Partitions []uint64
	Backups    []uint64
}

func (r *RoutingTable) prepareLeftOverDataReport() ([]byte, error) {
	res := leftOverDataReport{}
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		part := r.primary.PartitionById(partID)
		if part.Length() != 0 {
			res.Partitions = append(res.Partitions, partID)
		}

		backup := r.backup.PartitionById(partID)
		if backup.Length() != 0 {
			res.Backups = append(res.Backups, partID)
		}
	}
	return msgpack.Marshal(res)
}

func (r *RoutingTable) updateRoutingTableOnMember(data []byte, member discovery.Member) (*leftOverDataReport, error) {
	req := protocol.NewSystemMessage(protocol.OpUpdateRouting)
	req.SetValue(data)
	req.SetExtra(protocol.UpdateRoutingExtra{
		CoordinatorID: r.this.ID,
	})
	// TODO: This blocks whole flow. Use timeout for smooth operation.
	resp, err := r.requestTo(member.String(), req)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to update routing table on %s: %v", member, err)
		return nil, err
	}

	report := leftOverDataReport{}
	err = msgpack.Unmarshal(resp.Value(), &report)
	if err != nil {
		r.log.V(3).Printf("[ERROR] Failed to call decode ownership report from %s: %v", member, err)
		return nil, err
	}
	return &report, nil
}

func (r *RoutingTable) updateRoutingTableOnCluster() (map[discovery.Member]*leftOverDataReport, error) {
	data, err := msgpack.Marshal(r.table)
	if err != nil {
		return nil, err
	}

	var mtx sync.Mutex
	var g errgroup.Group
	reports := make(map[discovery.Member]*leftOverDataReport)
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	r.Members().Range(func(id uint64, tmp discovery.Member) bool {
		member := tmp
		g.Go(func() error {
			if err := sem.Acquire(r.ctx, 1); err != nil {
				r.log.V(3).Printf("[ERROR] Failed to acquire semaphore to update routing table on %s: %v", member, err)
				return err
			}
			defer sem.Release(1)

			report, err := r.updateRoutingTableOnMember(data, member)
			if err != nil {
				return err
			}

			mtx.Lock()
			defer mtx.Unlock()
			reports[member] = report
			return nil
		})
		return true
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return reports, nil
}
