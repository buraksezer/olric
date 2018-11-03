// Copyright 2018 Burak Sezer
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
	"errors"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack"
)

const eventChanCapacity = 32

var errHostNotFound = errors.New("host not found")

// discovery is a structure that encapsulates memberlist and
// provides useful functions to utilize it.
type discovery struct {
	logger     *log.Logger
	Name       string
	peers      []string
	memberlist *memberlist.Memberlist
	config     *memberlist.Config
	Birthdate  int64
	wg         sync.WaitGroup
	done       chan struct{}

	eventMx          sync.RWMutex
	eventsCh         chan memberlist.NodeEvent
	eventSubscribers []chan memberlist.NodeEvent
}

// TODO: NodeMetadata will be removed.
type NodeMetadata struct {
	Birthdate int64
}

// host represents a node in the cluster.
type host struct {
	NodeMetadata
	Name string
}

func (m host) String() string {
	return m.Name
}

func (d *discovery) DecodeMeta(buf []byte) (*NodeMetadata, error) {
	res := &NodeMetadata{}
	err := msgpack.Unmarshal(buf, res)
	return res, err
}

// New creates a new memberlist with a proper configuration and returns a new discovery instance along with it.
func newDiscovery(cfg *Config) (*discovery, error) {
	birthdate := time.Now().UnixNano()
	dlg, err := newDelegate(birthdate)
	if err != nil {
		return nil, err
	}
	eventsCh := make(chan memberlist.NodeEvent, eventChanCapacity)

	cfg.MemberlistConfig.Name = cfg.Name
	cfg.MemberlistConfig.Delegate = dlg
	cfg.MemberlistConfig.Logger = cfg.Logger
	cfg.MemberlistConfig.Events = &memberlist.ChannelEventDelegate{
		Ch: eventsCh,
	}
	list, err := memberlist.Create(cfg.MemberlistConfig)
	if err != nil {
		return nil, err
	}
	return &discovery{
		logger:     cfg.Logger,
		Name:       cfg.MemberlistConfig.Name,
		Birthdate:  birthdate,
		memberlist: list,
		peers:      cfg.Peers,
		config:     cfg.MemberlistConfig,
		eventsCh:   eventsCh,
		done:       make(chan struct{}),
	}, nil
}

// join is used to take an existing Memberlist and attempt to join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause remote
// nodes to become aware of the existence of this node, effectively joining the cluster.
func (d *discovery) join() {
	if len(d.peers) != 0 {
		nr, err := d.memberlist.Join(d.peers)
		if err != nil {
			d.logger.Printf("[WARN] There are some errors: %v", err)
		}
		if nr == 0 {
			d.logger.Println("[WARN] Join failed. Running as standalone")
		} else {
			d.logger.Printf("[INFO] The number of hosts successfully contacted: %d", nr)
		}
	}
	d.wg.Add(1)
	go d.eventLoop()
}

// getMembers returns a list of all known live nodes.
func (d *discovery) getMembers() []host {
	members := []host{}
	nodes := d.memberlist.Members()
	for _, node := range nodes {
		mt, _ := d.DecodeMeta(node.Meta)
		member := host{
			Name:         node.Name,
			NodeMetadata: *mt,
		}
		members = append(members, member)
	}

	// sort members by birthdate
	sort.Slice(members, func(i int, j int) bool {
		return members[i].Birthdate < members[j].Birthdate
	})
	return members
}

func (d *discovery) numMembers() int {
	return len(d.memberlist.Members())
}

// findMember finds and returns an alive member.
func (d *discovery) findMember(name string) (host, error) {
	members := d.getMembers()
	for _, member := range members {
		if member.Name == name {
			return member, nil
		}
	}
	return host{}, errHostNotFound
}

// getCoordinator returns the oldest node in the memberlist.
func (d *discovery) getCoordinator() host {
	members := d.getMembers()
	// That's not dangerous because memberlist includes the node's itself at least.
	return members[0]
}

// isCoordinator returns true if the caller is the coordinator node.
func (d *discovery) isCoordinator() bool {
	return d.getCoordinator().Name == d.Name
}

// localNode is used to return the local Node
func (d *discovery) localNode() *memberlist.Node {
	return d.memberlist.LocalNode()
}

// shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (d *discovery) shutdown() error {
	select {
	case <-d.done:
		return nil
	default:
	}
	close(d.done)
	// TODO: We may want to add a timeout for this.
	d.wg.Wait()
	return d.memberlist.Shutdown()
}

func (d *discovery) handleEvent(event memberlist.NodeEvent) {
	d.eventMx.RLock()
	defer d.eventMx.RUnlock()

	for _, ch := range d.eventSubscribers {
		if event.Node.Name == d.Name {
			continue
		}
		if event.Event != memberlist.NodeUpdate {
			ch <- event
			continue
		}
		// Overwrite it. In olric, NodeUpdate evaluated as NodeLeave
		event.Event = memberlist.NodeLeave
		ch <- event
		// Create a Join event from copied event.
		cpy := event
		cpy.Event = memberlist.NodeJoin
		ch <- cpy
		continue
	}
}

func (d *discovery) eventLoop() {
	defer d.wg.Done()

	for {
		select {
		case event := <-d.eventsCh:
			d.handleEvent(event)
		case <-d.done:
			return
		}
	}
}

func (d *discovery) subscribeNodeEvents() chan memberlist.NodeEvent {
	d.eventMx.Lock()
	defer d.eventMx.Unlock()

	ch := make(chan memberlist.NodeEvent, eventChanCapacity)
	d.eventSubscribers = append(d.eventSubscribers, ch)
	return ch
}

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte
}

// newDelegate returns a new delegate instance.
func newDelegate(birthdate int64) (delegate, error) {
	mt := &NodeMetadata{
		Birthdate: birthdate,
	}
	data, err := msgpack.Marshal(mt)
	if err != nil {
		return delegate{}, err
	}
	return delegate{
		meta: data,
	}, nil
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (d delegate) NodeMeta(limit int) []byte {
	return d.meta
}

// NotifyMsg is called when a user-data message is received.
func (d delegate) NotifyMsg(data []byte) {}

// GetBroadcasts is called when user data messages can be broadcast.
func (d delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState is used for a TCP Push/Pull.
func (d delegate) LocalState(join bool) []byte { return nil }

// MergeRemoteState is invoked after a TCP Push/Pull.
func (d delegate) MergeRemoteState(buf []byte, join bool) {}
