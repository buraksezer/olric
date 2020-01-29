// Copyright 2018-2019 Burak Sezer
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

/*Package discovery provides a basic memberlist integration.*/
package discovery

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/flog"
	"github.com/hashicorp/memberlist"
	"github.com/vmihailenco/msgpack"
)

const eventChanCapacity = 256

var ErrHostNotFound = errors.New("host not found")

// ClusterEvent is a single event related to node activity in the memberlist.
// The Node member of this struct must not be directly modified.
type ClusterEvent struct {
	Event    memberlist.NodeEventType
	NodeName string
	NodeAddr net.IP
	NodePort uint16
	NodeMeta []byte // Metadata from the delegate for this node.
}

func (c *ClusterEvent) MemberAddr() string {
	port := strconv.Itoa(int(c.NodePort))
	return net.JoinHostPort(c.NodeAddr.String(), port)
}

// Discovery is a structure that encapsulates memberlist and
// provides useful functions to utilize it.
type Discovery struct {
	log        *flog.Logger
	host       *Member
	memberlist *memberlist.Memberlist
	config     *config.Config

	// To manage Join/Leave/Update events
	clusterEventsMtx sync.RWMutex
	ClusterEvents    chan *ClusterEvent

	// Try to reconnect dead members
	deadMembers      map[string]int64
	deadMemberEvents chan *ClusterEvent

	eventSubscribers []chan *ClusterEvent

	// Flow control
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// Member represents a node in the cluster.
type Member struct {
	Name      string
	ID        uint64
	Birthdate int64
}

func (m Member) String() string {
	return m.Name
}

func (d *Discovery) DecodeNodeMeta(buf []byte) (Member, error) {
	res := &Member{}
	err := msgpack.Unmarshal(buf, res)
	return *res, err
}

// New creates a new memberlist with a proper configuration and returns a new Discovery instance along with it.
func New(log *flog.Logger, c *config.Config) *Discovery {
	// Calculate host's identity. It's useful to compare hosts.
	birthdate := time.Now().UnixNano()
	buf := make([]byte, 8+len(c.Name))
	binary.BigEndian.PutUint64(buf, uint64(birthdate))
	buf = append(buf, []byte(c.Name)...)
	id := c.Hasher.Sum64(buf)

	host := &Member{
		Name:      c.Name,
		ID:        id,
		Birthdate: birthdate,
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Discovery{
		host:        host,
		config:      c,
		log:         log,
		deadMembers: make(map[string]int64),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (d *Discovery) dialDeadMember(member string) {
	// Knock knock
	// TODO: Make this parametric
	conn, err := net.DialTimeout("tcp", member, 100*time.Millisecond)
	if err != nil {
		d.log.V(5).Printf("[ERROR] Failed to dial member: %s: %v", member, err)
		return
	}
	err = conn.Close()
	if err != nil {
		d.log.V(5).Printf("[ERROR] Failed to close connection: %s: %v", member, err)
		// network partitioning continues
		return
	}
	// Everything seems fine. Try to re-join!
	_, err = d.Rejoin([]string{member})
	if err != nil {
		d.log.V(5).Printf("[ERROR] Failed to re-join: %s: %v", member, err)
	}
}

func (d *Discovery) deadMemberTracker() {
	d.wg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		case e := <-d.deadMemberEvents:
			member := e.MemberAddr()
			if e.Event == memberlist.NodeJoin {
				delete(d.deadMembers, member)
			} else if e.Event == memberlist.NodeLeave {
				d.deadMembers[member] = time.Now().UnixNano()
			} else {
				d.log.V(2).Printf("[ERROR] Unknown memberlist event received for: %s: %v",
					e.NodeName, e.Event)
			}
		case <-time.After(time.Second):
			// TODO: make this parametric
			// Try to reconnect a random dead member every second.
			// The Go runtime selects a random item in the map
			for member, timestamp := range d.deadMembers {
				d.dialDeadMember(member)
				// TODO: Make this parametric
				if time.Now().Add(24*time.Hour).UnixNano() >= timestamp {
					delete(d.deadMembers, member)
				}
				break
			}
			// Just try one item
		}
	}
}

func (d *Discovery) Start() error {
	// ClusterEvents chan is consumed by the Olric package to maintain a consistent hash ring.
	d.ClusterEvents = d.SubscribeNodeEvents()
	d.deadMemberEvents = d.SubscribeNodeEvents()

	// Initialize a new memberlist
	dl, err := d.newDelegate()
	if err != nil {
		return err
	}
	eventsCh := make(chan memberlist.NodeEvent, eventChanCapacity)
	d.config.MemberlistConfig.Name = d.config.Name
	d.config.MemberlistConfig.Delegate = dl
	d.config.MemberlistConfig.Logger = d.config.Logger
	d.config.MemberlistConfig.Events = &memberlist.ChannelEventDelegate{
		Ch: eventsCh,
	}
	list, err := memberlist.Create(d.config.MemberlistConfig)
	if err != nil {
		return err
	}
	d.memberlist = list

	d.wg.Add(2)
	go d.eventLoop(eventsCh)
	go d.deadMemberTracker()
	return nil
}

// Join is used to take an existing Memberlist and attempt to Join a cluster
// by contacting all the given hosts and performing a state sync. Initially,
// the Memberlist only contains our own state, so doing this will cause remote
// nodes to become aware of the existence of this node, effectively joining the cluster.
func (d *Discovery) Join() (int, error) {
	return d.memberlist.Join(d.config.Peers)
}

func (d *Discovery) Rejoin(peers []string) (int, error) {
	return d.memberlist.Join(peers)
}

// GetMembers returns a list of all known live nodes.
func (d *Discovery) GetMembers() []Member {
	var members []Member
	nodes := d.memberlist.Members()
	for _, node := range nodes {
		member, _ := d.DecodeNodeMeta(node.Meta)
		members = append(members, member)
	}

	// sort members by birthdate
	sort.Slice(members, func(i int, j int) bool {
		return members[i].Birthdate < members[j].Birthdate
	})
	return members
}

func (d *Discovery) NumMembers() int {
	return d.memberlist.NumMembers()
}

// FindMemberByName finds and returns an alive member.
func (d *Discovery) FindMemberByName(name string) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.Name == name {
			return member, nil
		}
	}
	return Member{}, ErrHostNotFound
}

// FindMemberByID finds and returns an alive member.
func (d *Discovery) FindMemberByID(id uint64) (Member, error) {
	members := d.GetMembers()
	for _, member := range members {
		if member.ID == id {
			return member, nil
		}
	}
	return Member{}, ErrHostNotFound
}

// GetCoordinator returns the oldest node in the memberlist.
func (d *Discovery) GetCoordinator() Member {
	members := d.GetMembers()
	if len(members) == 0 {
		d.log.V(1).Printf("[ERROR] There is no member in memberlist")
		return Member{}
	}
	return members[0]
}

// IsCoordinator returns true if the caller is the coordinator node.
func (d *Discovery) IsCoordinator() bool {
	return d.GetCoordinator().ID == d.host.ID
}

// LocalNode is used to return the local Node
func (d *Discovery) LocalNode() *memberlist.Node {
	return d.memberlist.LocalNode()
}

// Shutdown will stop any background maintenance of network activity
// for this memberlist, causing it to appear "dead". A leave message
// will not be broadcasted prior, so the cluster being left will have
// to detect this node's Shutdown using probing. If you wish to more
// gracefully exit the cluster, call Leave prior to shutting down.
//
// This method is safe to call multiple times.
func (d *Discovery) Shutdown() error {
	select {
	case <-d.ctx.Done():
		return nil
	default:
	}
	d.cancel()
	// We don't do that in a goroutine with a timeout mechanism
	// because this mechanism may cause goroutine leak.
	d.wg.Wait()

	// Leave will broadcast a leave message but will not shutdown the background
	// listeners, meaning the node will continue participating in gossip and state
	// updates.
	d.log.V(2).Printf("[INFO] Broadcasting a leave message")
	if err := d.memberlist.Leave(15 * time.Second); err != nil {
		d.log.V(3).Printf("[ERROR] memberlist.Leave returned an error: %v", err)
	}
	return d.memberlist.Shutdown()
}

func convertToClusterEvent(e memberlist.NodeEvent) *ClusterEvent {
	return &ClusterEvent{
		Event:    e.Event,
		NodeName: e.Node.Name,
		NodeAddr: e.Node.Addr,
		NodePort: e.Node.Port,
		NodeMeta: e.Node.Meta,
	}
}

func (d *Discovery) handleEvent(event memberlist.NodeEvent) {
	d.clusterEventsMtx.RLock()
	defer d.clusterEventsMtx.RUnlock()

	for _, ch := range d.eventSubscribers {
		if event.Node.Name == d.host.Name {
			continue
		}
		// NodeJoin or NodeLeave
		if event.Event != memberlist.NodeUpdate {
			ch <- convertToClusterEvent(event)
			continue
		}

		// NodeUpdate: Olric is an in-memory k/v store. If the node metadata has been updated,
		// the node may be restarted or/and serves stale data.
		e := convertToClusterEvent(event)
		e.Event = memberlist.NodeLeave
		ch <- e
		// Create a Join event from copied event.
		e = convertToClusterEvent(event)
		e.Event = memberlist.NodeJoin
		ch <- e
	}
}

// eventLoop awaits for messages from memberlist and broadcasts them to  event listeners.
func (d *Discovery) eventLoop(eventsCh chan memberlist.NodeEvent) {
	defer d.wg.Done()

	for {
		select {
		case e := <-eventsCh:
			d.handleEvent(e)
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *Discovery) SubscribeNodeEvents() chan *ClusterEvent {
	d.clusterEventsMtx.Lock()
	defer d.clusterEventsMtx.Unlock()

	ch := make(chan *ClusterEvent, eventChanCapacity)
	d.eventSubscribers = append(d.eventSubscribers, ch)
	return ch
}

// delegate is a struct which implements memberlist.Delegate interface.
type delegate struct {
	meta []byte
}

// newDelegate returns a new delegate instance.
func (d *Discovery) newDelegate() (delegate, error) {
	data, err := msgpack.Marshal(d.host)
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
