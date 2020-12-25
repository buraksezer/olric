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
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/environment"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/checkpoint"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/hashicorp/memberlist"
)

// ErrClusterQuorum means that the cluster could not reach a healthy numbers of members to operate.
var ErrClusterQuorum = errors.New("cannot be reached cluster quorum to operate")

type route struct {
	Owners  []discovery.Member
	Backups []discovery.Member
}

type RoutingTable struct {
	sync.RWMutex // routingMtx

	// These values is useful to control operation status.
	bootstrapped int32

	updateRoutingMtx sync.Mutex

	table map[uint64]*route

	// consistent hash ring implementation.
	consistent *consistent.Consistent

	// numMembers is used to check cluster quorum.
	numMembers int32
	// Currently owned partition count. Approximate LRU implementation
	// uses that.
	ownedPartitionCount uint64
	signature           uint64
	this                discovery.Member
	members             *members
	config              *config.Config
	log                 *flog.Logger
	primary             *partitions.Partitions
	backup              *partitions.Partitions
	client              *transport.Client
	discovery           *discovery.Discovery
	callbacks           []func()
	callbackMtx         sync.Mutex
	updatePeriod        time.Duration
	updateMtx           sync.Mutex
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

func New(e *environment.Environment) *RoutingTable {
	checkpoint.Add()
	c := e.Get("config").(*config.Config)
	log := e.Get("logger").(*flog.Logger)

	ctx, cancel := context.WithCancel(context.Background())
	cc := consistent.Config{
		Hasher:            c.Hasher,
		PartitionCount:    int(c.PartitionCount),
		ReplicationFactor: 20, // TODO: This also may be a configuration param.
		Load:              c.LoadFactor,
	}
	return &RoutingTable{
		members:      newMembers(),
		discovery:    discovery.New(log, c),
		config:       c,
		log:          log,
		consistent:   consistent.New(nil, cc),
		primary:      e.Get("primary").(*partitions.Partitions),
		backup:       e.Get("backup").(*partitions.Partitions),
		client:       e.Get("client").(*transport.Client),
		updatePeriod: time.Minute,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (r *RoutingTable) Discovery() *discovery.Discovery {
	return r.discovery
}

func (r *RoutingTable) This() discovery.Member {
	return r.this
}

// setNumMembers assigns the current number of members in the cluster to a variable.
func (r *RoutingTable) setNumMembers() {
	// Calling NumMembers in every request is quite expensive.
	// It's rarely updated. Just call this when the membership info changed.
	nr := int32(r.discovery.NumMembers())
	atomic.StoreInt32(&r.numMembers, nr)
}

func (r *RoutingTable) SetNumMembersEagerly(nr int32) {
	atomic.StoreInt32(&r.numMembers, nr)
}

func (r *RoutingTable) NumMembers() int32 {
	return atomic.LoadInt32(&r.numMembers)
}

func (r *RoutingTable) Members() *members {
	return r.members
}

func (r *RoutingTable) setSignature(s uint64) {
	atomic.StoreUint64(&r.signature, s)
}

func (r *RoutingTable) Signature() uint64 {
	return atomic.LoadUint64(&r.signature)
}

func (r *RoutingTable) setOwnedPartitionCount() {
	var count uint64
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		part := r.primary.PartitionById(partID)
		if part.Owner().CompareByID(r.this) {
			count++
		}
	}
	atomic.StoreUint64(&r.ownedPartitionCount, count)
}

func (r *RoutingTable) OwnedPartitionCount() uint64 {
	return atomic.LoadUint64(&r.ownedPartitionCount)
}

func (r *RoutingTable) CheckMemberCountQuorum() error {
	// This type of quorum function determines the presence of quorum based on the count of members in the cluster,
	// as observed by the local member’s cluster membership manager
	if r.config.MemberCountQuorum > r.NumMembers() {
		return ErrClusterQuorum
	}
	return nil
}

func (r *RoutingTable) markBootstrapped() {
	// Bootstrapped by the coordinator.
	atomic.StoreInt32(&r.bootstrapped, 1)
}

func (r *RoutingTable) IsBootstrapped() bool {
	// Bootstrapped by the coordinator.
	return atomic.LoadInt32(&r.bootstrapped) == 1
}

// checkBootstrap is called for every request and checks whether the node is bootstrapped.
// It has to be very fast for a smooth operation.
func (r *RoutingTable) CheckBootstrap() error {
	ctx, cancel := context.WithTimeout(context.Background(), r.config.BootstrapTimeout)
	defer cancel()
	return r.tryWithInterval(ctx, 100*time.Millisecond, func() error {
		if r.IsBootstrapped() {
			return nil
		}
		// Final error
		return ErrOperationTimeout
	})
}

func (r *RoutingTable) fillRoutingTable() {
	table := make(map[uint64]*route)
	for partID := uint64(0); partID < r.config.PartitionCount; partID++ {
		rt := &route{
			Owners: r.distributePrimaryCopies(partID),
		}
		if r.config.ReplicaCount > config.MinimumReplicaCount {
			rt.Backups = r.distributeBackups(partID)
		}
		table[partID] = rt
	}
	r.table = table
}

func (r *RoutingTable) UpdateRoutingEagerly() {
	r.updateRouting()
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
	if r.config.MemberCountQuorum > r.NumMembers() {
		r.log.V(2).Printf("[ERROR] Impossible to calculate and update routing table: %v", ErrClusterQuorum)
		return
	}

	r.fillRoutingTable()
	reports, err := r.updateRoutingTableOnCluster()
	if err != nil {
		r.log.V(2).Printf("[ERROR] Failed to update routing table on cluster: %v", err)
		return
	}
	r.processLeftOverDataReports(reports)
}

func (r *RoutingTable) processClusterEvent(event *discovery.ClusterEvent) {
	r.Members().Lock()
	defer r.Members().Unlock()

	member, _ := discovery.NewMemberFromMetadata(event.NodeMeta)

	switch event.Event {
	case memberlist.NodeJoin:
		r.Members().Add(member)
		r.consistent.Add(member)
		r.log.V(2).Printf("[INFO] Node joined: %s", member)
	case memberlist.NodeLeave:
		if _, err := r.Members().Get(member.ID); err != nil {
			r.log.V(2).Printf("[ERROR] Unknown node left: %s: %d", event.NodeName, member.ID)
			return
		}
		r.Members().Delete(member.ID)
		r.consistent.Remove(event.NodeName)
		// Don't try to used closed sockets again.
		r.client.ClosePool(event.NodeName)
		r.log.V(2).Printf("[INFO] Node left: %s", event.NodeName)
	case memberlist.NodeUpdate:
		// Node's birthdate may be changed. Close the pool and re-add to the hash ring.
		// This takes linear time, but member count should be too small for a decent computer!
		r.Members().Range(func(id uint64, item discovery.Member) bool {
			if member.CompareByName(item) {
				r.Members().Delete(id)
				r.consistent.Remove(event.NodeName)
				r.client.ClosePool(event.NodeName)
			}
			return true
		})
		r.Members().Add(member)
		r.consistent.Add(member)
		r.log.V(2).Printf("[INFO] Node updated: %s", member)
	default:
		r.log.V(2).Printf("[ERROR] Unknown event received: %v", event)
		return
	}

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	r.setNumMembers()
}

func (r *RoutingTable) listenClusterEvents(eventCh chan *discovery.ClusterEvent) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case e := <-eventCh:
			r.processClusterEvent(e)
			r.updateRouting()
		}
	}
}

func (r *RoutingTable) updatePeriodically() {
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
}

func (r *RoutingTable) requestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	resp, err := r.client.RequestTo(addr, req)
	if err != nil {
		return nil, err
	}
	status := resp.Status()
	if status == protocol.StatusOK {
		return resp, nil
	}
	return nil, transport.NewOpError(status, string(resp.Value()))
}

func (r *RoutingTable) Start() error {
	err := r.discovery.Start()
	if err != nil {
		return err
	}

	err = r.attemptToJoin()
	if err == ErrClusterJoin {
		r.log.V(1).Printf("[INFO] Forming a new Olric cluster")
		err = nil
	}
	if err != nil {
		return err
	}

	this, err := r.discovery.FindMemberByName(r.config.MemberlistConfig.Name)
	if err != nil {
		r.log.V(2).Printf("[ERROR] Failed to get this node in cluster: %v", err)
		serr := r.discovery.Shutdown()
		if serr != nil {
			return serr
		}
		return err
	}
	r.this = this

	// Store the current number of members in the member list.
	// We need this to implement a simple split-brain protection algorithm.
	r.setNumMembers()

	r.wg.Add(1)
	go r.listenClusterEvents(r.discovery.ClusterEvents)

	// 1 Hour
	ctx, cancel := context.WithTimeout(r.ctx, time.Hour)
	defer cancel()
	err = r.tryWithInterval(ctx, time.Second, func() error {
		// Check member count quorum now. If there is no enough peers to work, wait forever.
		err := r.CheckMemberCountQuorum()
		if err != nil {
			r.log.V(2).Printf("[ERROR] Inoperable node: %v", err)
		}
		return err
	})
	if err != nil {
		return err
	}

	r.Members().Add(r.this)
	r.consistent.Add(r.this)

	if r.discovery.IsCoordinator() {
		err = r.bootstrapCoordinator()
		if err != err {
			return err
		}
	}

	r.wg.Add(1)
	go r.updatePeriodically()

	if r.config.MemberlistInterface != "" {
		r.log.V(2).Printf("[INFO] Memberlist uses interface: %s", r.config.MemberlistInterface)
	}
	r.log.V(2).Printf("[INFO] Memberlist bindAddr: %s, bindPort: %d", r.config.MemberlistConfig.BindAddr, r.config.MemberlistConfig.BindPort)
	r.log.V(2).Printf("[INFO] Cluster coordinator: %s", r.discovery.GetCoordinator())
	checkpoint.Pass()
	return nil
}

func (r *RoutingTable) Shutdown(ctx context.Context) error {
	if err := r.discovery.Shutdown(); err != nil {
		return err
	}

	r.cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return err
		}
	case <-done:
	}
	return nil
}
