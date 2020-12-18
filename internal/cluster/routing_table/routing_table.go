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
	"context"
	"errors"
	"fmt"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/discovery"
)

// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
var ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

type route struct {
	Owners  []discovery.Member
	Backups []discovery.Member
}

type RoutingTable struct {
	sync.RWMutex // routingMtx

	table map[uint64]route

	// consistent hash ring implementation.
	consistent *consistent.Consistent

	// numMembers is used to check cluster quorum.
	numMembers   int32
	signature    uint64
	primary      *partitions.Partitions
	backup       *partitions.Partitions
	discovery    *discovery.Discovery
	config       *config.Config
	updatePeriod time.Duration
	updateMtx    sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func New(c *config.Config, primary, backup *partitions.Partitions, discovery *discovery.Discovery) *RoutingTable {
	ctx, cancel := context.WithCancel(context.Background())

	cc := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}

	return &RoutingTable{
		config:       c,
		consistent:   consistent.New(nil, cc),
		primary:      primary,
		backup:       backup,
		discovery:    discovery,
		updatePeriod: time.Second,
		table:        make(map[uint64]route),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (r *RoutingTable) SetSignature(s uint64) {
	r.signature = s
}

func (r *RoutingTable) Signature() uint64 {
	return r.signature
}

func (r *RoutingTable) CheckMemberCountQuorum() error {
	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	nr := atomic.LoadInt32(&r.numMembers)
	if r.config.MemberCountQuorum > nr {
		return ErrClusterQuorum
	}
	return nil
}

func (r *RoutingTable) BootstrapRoutingTable() error {
	if r.table != nil {
		return errors.New("routing table had already been bootstrapped")
	}
	r.fillRoutingTable()
	return nil
}

func (r *RoutingTable) fillRoutingTable() {
	r.table = make(map[uint64]route)
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		item := r.table[partID]
		//item.Owners = db.distributePrimaryCopies(partID)
		//if db.config.ReplicaCount > config.MinimumReplicaCount {
		//	item.Backups = db.distributeBackups(partID)
		//}
		r.table[partID] = item
	}
}

func (r *RoutingTable) updateRouting() {
	// This function is called by listenMemberlistEvents and updateRoutingPeriodically
	// So this lock prevents parallel execution.
	r.Lock()
	defer r.Unlock()

	// This function is only run by the cluster coordinator.
	if !r.discovery.IsCoordinator() {
		return
	}

	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	nr := atomic.LoadInt32(&r.numMembers)
	if r.config.MemberCountQuorum > nr {
		//db.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", ErrClusterQuorum)
		return
	}

	r.fillRoutingTable()

	/*table := db.distributePartitions()
	reports, err := db.updateRoutingTableOnCluster(table)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
		return
	}
	db.processOwnershipReports(reports)*/
}

func (r *RoutingTable) ListenClusterEvents(eventCh chan *discovery.ClusterEvent) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.ctx.Done():
				return
			case _ = <-eventCh:
				// db.processClusterEvent(e)
				r.updateRouting()
			}
		}
	}()
}

func (r *RoutingTable) UpdatePeriodically() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.updatePeriod)
		defer ticker.Stop()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				r.updateRouting()
			}
		}
	}()
}

func (r *RoutingTable) Close() {
	r.cancel()
	// TODO: Add graceperiod
	r.wg.Wait()
}

func (r *RoutingTable) requestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	resp, err := r.client.RequestTo(addr, req)
	if err != nil {
		return nil, err
	}

	status := resp.Status()

	switch {
	case status == protocol.StatusOK:
		return resp, nil
	case status == protocol.StatusInternalServerError:
		return nil, errors.Wrap(ErrInternalServerError, string(resp.Value()))
	case status == protocol.StatusErrNoSuchLock:
		return nil, ErrNoSuchLock
	case status == protocol.StatusErrLockNotAcquired:
		return nil, ErrLockNotAcquired
	case status == protocol.StatusErrKeyNotFound:
		return nil, ErrKeyNotFound
	case status == protocol.StatusErrWriteQuorum:
		return nil, ErrWriteQuorum
	case status == protocol.StatusErrReadQuorum:
		return nil, ErrReadQuorum
	case status == protocol.StatusErrOperationTimeout:
		return nil, ErrOperationTimeout
	case status == protocol.StatusErrKeyFound:
		return nil, ErrKeyFound
	case status == protocol.StatusErrClusterQuorum:
		return nil, ErrClusterQuorum
	case status == protocol.StatusErrEndOfQuery:
		return nil, ErrEndOfQuery
	case status == protocol.StatusErrUnknownOperation:
		return nil, ErrUnknownOperation
	case status == protocol.StatusErrServerGone:
		return nil, ErrServerGone
	case status == protocol.StatusErrInvalidArgument:
		return nil, ErrInvalidArgument
	case status == protocol.StatusErrKeyTooLarge:
		return nil, ErrKeyTooLarge
	case status == protocol.StatusErrNotImplemented:
		return nil, ErrNotImplemented
	}
	return nil, fmt.Errorf("unknown status code: %d", status)
}