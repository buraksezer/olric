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
	"time"

	"github.com/buraksezer/consistent"
	"github.com/buraksezer/olric/internal/discovery"
)

// bootstrapCoordinator prepares the very first routing table and bootstraps the coordinator node.
func (r *RoutingTable) bootstrapCoordinator() error {
	r.Lock()
	defer r.Unlock()

	r.fillRoutingTable()
	_, err := r.updateRoutingTableOnCluster()
	if err != nil {
		return err
	}
	// The coordinator bootstraps itself.
	r.markBootstrapped()
	r.log.V(2).Printf("[INFO] The cluster coordinator has been bootstrapped")
	return nil
}

func (r *RoutingTable) Discovery() *discovery.Discovery {
	return r.discovery
}

func (r *RoutingTable) This() discovery.Member {
	return r.this
}

func (r *RoutingTable) attemptToJoin() {
	attempts := 0
	for attempts < r.config.MaxJoinAttempts {
		select {
		case <-r.ctx.Done():
			// The node is gone.
			return
		default:
		}

		attempts++
		n, err := r.discovery.Join()
		if err == nil {
			r.log.V(2).Printf("[INFO] Join completed. Synced with %d initial nodes", n)
			break
		}

		r.log.V(2).Printf("[ERROR] Join attempt returned error: %s", err)
		if r.IsBootstrapped() {
			r.log.V(2).Printf("[INFO] Bootstrapped by the cluster coordinator")
			break
		}

		r.log.V(2).Printf("[INFO] Awaits for %s to join again (%d/%d)",
			r.config.JoinRetryInterval, attempts, r.config.MaxJoinAttempts)
		<-time.After(r.config.JoinRetryInterval)
	}
}

func (r *RoutingTable) checkOperatingStatus() {
	// Check member count quorum now. If there is no enough peers to work, wait forever.
	for {
		err := r.CheckMemberCountQuorum()
		if err == nil {
			// It's OK. Continue as usual.
			break
		}

		r.log.V(2).Printf("[ERROR] Inoperable node: %v", err)
		select {
		// TODO: Consider making this parametric
		case <-time.After(time.Second):
			// try again
		case <-r.ctx.Done():
			// the server is gone
			return
		}
	}
}

func (r *RoutingTable) initialize() error {
	r.Members().Add(r.this)
	r.consistent.Add(r.this)
	if !r.discovery.IsCoordinator() {
		return nil
	}
	err := r.bootstrapCoordinator()
	if err == consistent.ErrInsufficientMemberCount {
		r.log.V(2).Printf("[ERROR] Failed to bootstrap the coordinator node: %v", err)
		// Olric will try to form a cluster again.
		err = nil
	}
	return err
}

func (r *RoutingTable) Start() error {
	d, err := discovery.New(r.log, r.config)
	if err != nil {
		return err
	}
	err = d.Start()
	if err != nil {
		return err
	}
	r.discovery = d

	r.attemptToJoin()
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
	go r.listenClusterEvents(d.ClusterEvents)
	r.checkOperatingStatus()
	if err = r.initialize(); err != nil {
		return err
	}
	r.updatePeriodically()

	if r.config.Interface != "" {
		r.log.V(2).Printf("[INFO] Olric uses interface: %s", r.config.Interface)
	}
	r.log.V(2).Printf("[INFO] Olric bindAddr: %s, bindPort: %d", r.config.BindAddr, r.config.BindPort)
	if r.config.MemberlistInterface != "" {
		r.log.V(2).Printf("[INFO] Memberlist uses interface: %s", r.config.MemberlistInterface)
	}
	r.log.V(2).Printf("[INFO] Memberlist bindAddr: %s, bindPort: %d", r.config.MemberlistConfig.BindAddr, r.config.MemberlistConfig.BindPort)
	r.log.V(2).Printf("[INFO] Cluster coordinator: %s", r.discovery.GetCoordinator())
	return nil
}
