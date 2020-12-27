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

package dtopics

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrUnknownOperation means that an unidentified message has been received from a client.
	ErrUnknownOperation = errors.New("unknown operation")
	ErrNotImplemented   = errors.New("not implemented")
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")
)

const (
	// Messages are delivered in random order. It's good to distribute independent events in a distributed system.
	UnorderedDelivery = int16(1) << iota

	// Messages are delivered in some order. Not implemented yet.
	OrderedDelivery
)

var errListenerIDCollision = errors.New("given listenerID already exists")

// Message is a message type for DTopic data structure.
type Message struct {
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
	ds          *DTopics
}

// NewDTopic returns a new distributed topic instance.
// Parameters:
//   * name: DTopic name.
//   * concurrency: Maximum number of concurrently processing DTopic messages.
//   * flag: Any flag to control DTopic behaviour.
// Flags for delivery options:
//   * UnorderedDelivery: Messages are delivered in random order. It's good to distribute independent events in a distributed system.
//   * OrderedDelivery: Messages are delivered in order. Not implemented yet.
func (ds *DTopics) NewDTopic(name string, concurrency int, flag int16) (*DTopic, error) {
	ds.Lock()
	defer ds.Unlock()

	if dt, ok := ds.m[name]; ok {
		return dt, nil
	}

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
	if err := ds.rt.CheckMemberCountQuorum(); err != nil {
		return nil, err
	}
	// An Olric node has to be bootstrapped to function properly.
	if err := ds.rt.CheckBootstrap(); err != nil {
		return nil, err
	}

	dt := &DTopic{
		name:        name,
		flag:        flag,
		concurrency: concurrency,
		ds:          ds,
	}

	ds.m[name] = dt
	return dt, nil
}

// Publish publishes the given message to listeners of the topic. Message order and delivery are not guaranteed.
func (d *DTopic) Publish(msg interface{}) error {
	tm := &Message{
		Message:       msg,
		PublisherAddr: d.ds.rt.This().String(),
		PublishedAt:   time.Now().UnixNano(),
	}
	return d.ds.publishDTopicMessage(d.name, tm)
}

// AddListener adds a new listener for the topic. Returns a registration ID or a non-nil error.
// Registered functions are run by parallel.
func (d *DTopic) AddListener(f func(Message)) (uint64, error) {
	return d.ds.dispatcher.addListener(d.name, d.concurrency, f)
}

// RemoveListener removes a listener with the given listenerID.
func (d *DTopic) RemoveListener(listenerID uint64) error {
	return d.ds.dispatcher.removeListener(d.name, listenerID)
}

// Destroy removes all listeners for this topic on the cluster. If Publish function is called again after Destroy, the topic will be
// recreated.
func (d *DTopic) Destroy() error {
	return d.ds.destroyDTopicOnCluster(d.name)
}
