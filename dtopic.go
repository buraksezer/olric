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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	// Messages are delivered in random order. It's good to distribute independent events in a distributed system.
	UnorderedDelivery = int16(1) << iota

	// Messages are delivered in some order. Not implemented yet.
	OrderedDelivery
)

var errListenerIDCollision = errors.New("given listenerID already exists")

// DTopicMessage is a message type for DTopic data structure.
type DTopicMessage struct {
	Message       interface{}
	PublisherAddr string
	PublishedAt   int64
}

// DTopic implements a distributed topic to deliver messages between clients and Olric nodes. You should know that:
//
//  * Communication between parties is one-to-many (fan-out).
//  * All data is in-memory, and the published messages are not stored in the cluster.
//  * Fire&Forget: message delivery is not guaranteed.
type DTopic struct {
	name        string
	flag        int16
	concurrency int
	db          *Olric
}

type listener struct {
	f func(message DTopicMessage)
}

// Listeners with id.
type listeners struct {
	m           map[uint64]*listener
	concurrency int
}

// Registered listeners for topics. A global map for an Olric instance.
type topics struct {
	mtx sync.RWMutex
	m   map[string]*listeners
}

type dtopic struct {
	topics *topics
	ctx    context.Context
}

func newDTopic(ctx context.Context) *dtopic {
	return &dtopic{
		topics: &topics{m: make(map[string]*listeners)},
		ctx:    ctx,
	}
}

func (dt *dtopic) _addListener(listenerID uint64, topic string, concurrency int, f func(DTopicMessage)) error {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()

	l, ok := dt.topics.m[topic]
	if ok {
		if _, ok = l.m[listenerID]; ok {
			return errListenerIDCollision
		}
		l.m[listenerID] = &listener{f: f}
	} else {
		dt.topics.m[topic] = &listeners{
			m:           make(map[uint64]*listener),
			concurrency: concurrency,
		}
		dt.topics.m[topic].m[listenerID] = &listener{f: f}
	}
	return nil
}

func (dt *dtopic) addListener(topic string, concurrency int, f func(DTopicMessage)) (uint64, error) {
	listenerID := rand.Uint64()
	err := dt._addListener(listenerID, topic, concurrency, f)
	if err != nil {
		return 0, err
	}
	return listenerID, nil
}

// addRemoveListener registers clients listeners.
func (dt *dtopic) addRemoteListener(listenerID uint64, topic string, concurrency int, f func(DTopicMessage)) error {
	return dt._addListener(listenerID, topic, concurrency, f)
}

func (dt *dtopic) removeListener(topic string, listenerID uint64) error {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()

	l, ok := dt.topics.m[topic]
	if !ok {
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	_, ok = l.m[listenerID]
	if !ok {
		return fmt.Errorf("listener not found: %s: %w", topic, ErrInvalidArgument)
	}

	delete(l.m, listenerID)
	if len(l.m) == 0 {
		delete(dt.topics.m, topic)
	}
	return nil
}

// dispatch sends a received message to all registered listeners. It doesn't store
// any message. It just dispatch the message.
func (dt *dtopic) dispatch(topic string, msg *DTopicMessage) error {
	dt.topics.mtx.RLock()
	defer dt.topics.mtx.RUnlock()

	l, ok := dt.topics.m[topic]
	if !ok {
		// there is no listener for this topic on this node.
		return fmt.Errorf("topic not found: %s: %w", topic, ErrInvalidArgument)
	}

	var wg sync.WaitGroup
	if l.concurrency == 0 {
		// the number of logical CPUs usable by the current process.
		l.concurrency = runtime.NumCPU()
	}
	sem := semaphore.NewWeighted(int64(l.concurrency))
	for _, ll := range l.m {
		if err := sem.Acquire(dt.ctx, 1); err != nil {
			return err
		}

		wg.Add(1)
		// Dereference the pointer and make a copy of DTopicMessage for every listener.
		go func(f func(message DTopicMessage)) {
			defer wg.Done()
			defer sem.Release(1)
			f(*msg)
		}(ll.f)
	}
	wg.Wait()
	return nil
}

func (dt *dtopic) destroy(topic string) {
	dt.topics.mtx.Lock()
	defer dt.topics.mtx.Unlock()
	delete(dt.topics.m, topic)
}

// NewDTopic returns a new distributed topic instance.
// Parameters:
//   * name: DTopic name.
//   * concurrency: Maximum number of concurrently processing DTopic messages.
//   * flag: Any flag to control DTopic behaviour.
// Flags for delivery options:
//   * UnorderedDelivery: Messages are delivered in random order. It's good to distribute independent events in a distributed system.
//   * OrderedDelivery: Messages are delivered in order. Not implemented yet.
func (db *Olric) NewDTopic(name string, concurrency int, flag int16) (*DTopic, error) {
	if flag&UnorderedDelivery == 0 && flag&OrderedDelivery == 0 {
		return nil, fmt.Errorf("invalid delivery mode: %w", ErrInvalidArgument)
	}
	if flag&OrderedDelivery != 0 {
		return nil, ErrNotImplemented
	}
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := db.checkOperationStatus(); err != nil {
		return nil, err
	}
	return &DTopic{
		name:        name,
		db:          db,
		flag:        flag,
		concurrency: concurrency,
	}, nil
}

func (db *Olric) publishDTopicMessageOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	var msg DTopicMessage
	err := msgpack.Unmarshal(req.Value(), &msg)
	if err != nil {
		db.errorResponse(w, err)
		return
	}

	err = db.dtopic.dispatch(req.DTopic(), &msg)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) publishDTopicMessageToAddr(member discovery.Member, topic string, msg *DTopicMessage, sem *semaphore.Weighted) error {
	defer db.wg.Done()
	defer sem.Release(1)

	if cmpMembersByID(member, db.this) {
		// Dispatch messages in this process.
		err := db.dtopic.dispatch(topic, msg)
		if err != nil {
			if db.log.V(6).Ok() {
				db.log.V(6).Printf("[ERROR] Failed to dispatch message on this node: %v", err)
			}
			if !errors.Is(err, ErrInvalidArgument) {
				return err
			}
			return nil
		}
		return nil
	}
	data, err := msgpack.Marshal(*msg)
	if err != nil {
		return err
	}
	req := protocol.NewDTopicMessage(protocol.OpPublishDTopicMessage)
	req.SetDTopic(topic)
	req.SetValue(data)
	_, err = db.requestTo(member.String(), req)
	if err != nil {
		db.log.V(2).Printf("[ERROR] Failed to publish message to %s: %v", member, err)
		return err
	}
	return nil
}

func (db *Olric) publishDTopicMessage(topic string, msg *DTopicMessage) error {
	db.members.mtx.RLock()
	defer db.members.mtx.RUnlock()

	// Propagate the message to the cluster in a parallel manner but
	// control concurrency. In order to prevent overloaded servers
	// because of network I/O, we use a semaphore.
	num := int64(runtime.NumCPU())
	sem := semaphore.NewWeighted(num)
	var g errgroup.Group
	for _, member := range db.members.m {
		if !db.isAlive() {
			return ErrServerGone
		}

		if err := sem.Acquire(db.ctx, 1); err != nil {
			db.log.V(3).Printf("[ERROR] Failed to acquire semaphore: %v", err)
			return err
		}

		db.wg.Add(1)
		member := member // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			return db.publishDTopicMessageToAddr(member, topic, msg, sem)
		})
	}
	// Wait blocks until all function calls from the Go method have returned,
	// then returns the first non-nil error (if any) from them.
	return g.Wait()
}

func (db *Olric) exDTopicAddListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	name := req.DTopic()
	streamID := req.Extra().(protocol.DTopicAddListenerExtra).StreamID
	db.streams.mu.RLock()
	str, ok := db.streams.m[streamID]
	db.streams.mu.RUnlock()
	if !ok {
		err := fmt.Errorf("%w: StreamID could not be found", ErrInvalidArgument)
		db.errorResponse(w, err)
		return
	}

	// Local listener
	listenerID := req.Extra().(protocol.DTopicAddListenerExtra).ListenerID

	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		select {
		case <-str.ctx.Done():
		case <-db.ctx.Done():
		}
		err := db.dtopic.removeListener(name, listenerID)
		if err != nil {
			db.log.V(4).Printf("[ERROR] ListenerID: %d could not be removed: %v", listenerID, err)
		}
		db.log.V(4).Printf("[INFO] ListenerID: %d has been removed", listenerID)
	}()

	f := func(msg DTopicMessage) {
		db.streams.mu.RLock()
		s, ok := db.streams.m[streamID]
		db.streams.mu.RUnlock()
		if !ok {
			db.log.V(4).Printf("[ERROR] Stream could not be found with the given StreamID: %d", streamID)
			err := db.dtopic.removeListener(name, listenerID)
			if err != nil {
				db.log.V(4).Printf("[ERROR] Listener could not be removed with ListenerID: %d: %v", listenerID, err)
			}
			return
		}
		value, err := msgpack.Marshal(msg)
		if err != nil {
			db.log.V(4).Printf("[ERROR] Failed to serialize DTopicMessage: %v", err)
			return
		}
		m := protocol.NewDMapMessage(protocol.OpStreamMessage)
		m.SetDMap(name)
		m.SetValue(value)
		m.SetExtra(protocol.StreamMessageExtra{
			ListenerID: listenerID,
		})
		s.write <- m
	}
	// set concurrency parameter as 0. the registered listener will only make network i/o. NumCPU is good for this.
	err := db.dtopic.addRemoteListener(listenerID, name, 0, f)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) exDTopicPublishOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	msg, err := db.unmarshalValue(req.Value())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	tm := &DTopicMessage{
		Message:       msg,
		PublisherAddr: "",
		PublishedAt:   time.Now().UnixNano(),
	}
	err = db.publishDTopicMessage(req.DTopic(), tm)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// Publish publishes the given message to listeners of the topic. Message order and delivery are not guaranteed.
func (dt *DTopic) Publish(msg interface{}) error {
	tm := &DTopicMessage{
		Message:       msg,
		PublisherAddr: dt.db.this.String(),
		PublishedAt:   time.Now().UnixNano(),
	}
	return dt.db.publishDTopicMessage(dt.name, tm)
}

// AddListener adds a new listener for the topic. Returns a registration ID or a non-nil error.
// Registered functions are run by parallel.
func (dt *DTopic) AddListener(f func(DTopicMessage)) (uint64, error) {
	return dt.db.dtopic.addListener(dt.name, dt.concurrency, f)
}

func (db *Olric) exDTopicRemoveListenerOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	extra, ok := req.Extra().(protocol.DTopicRemoveListenerExtra)
	if !ok {
		db.errorResponse(w, fmt.Errorf("%w: wrong extra type", ErrInvalidArgument))
		return
	}
	err := db.dtopic.removeListener(req.DTopic(), extra.ListenerID)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// RemoveListener removes a listener with the given listenerID.
func (dt *DTopic) RemoveListener(listenerID uint64) error {
	return dt.db.dtopic.removeListener(dt.name, listenerID)
}

func (db *Olric) destroyDTopicOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	// req.DMap() is topic name in this context. This confusion will be fixed.
	db.dtopic.destroy(req.DTopic())
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) destroyDTopicOnCluster(topic string) error {
	db.members.mtx.RLock()
	defer db.members.mtx.RUnlock()

	var g errgroup.Group
	for _, member := range db.members.m {
		if !db.isAlive() {
			return ErrServerGone
		}

		member := member // https://golang.org/doc/faq#closures_and_goroutines
		g.Go(func() error {
			req := protocol.NewDTopicMessage(protocol.OpDestroyDTopic)
			req.SetDTopic(topic)
			_, err := db.requestTo(member.String(), req)
			if err != nil {
				db.log.V(2).Printf("[ERROR] Failed to call Destroy on %s, topic: %s : %v", member, topic, err)
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func (db *Olric) exDTopicDestroyOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DTopicMessage)
	err := db.destroyDTopicOnCluster(req.DTopic())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

// Destroy removes all listeners for this topic on the cluster. If Publish function is called again after Destroy, the topic will be
// recreated.
func (dt *DTopic) Destroy() error {
	return dt.db.destroyDTopicOnCluster(dt.name)
}
