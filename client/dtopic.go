// Copyright 2018-2021 Burak Sezer
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

package client

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack"
	"golang.org/x/sync/semaphore"
)

// DTopic denotes a distributed topic instance in the cluster.
type DTopic struct {
	*Client
	name        string
	flag        int16
	concurrency int

	mu        sync.Mutex
	listeners map[uint64]struct{}
}

// NewDTopic returns a new distributed topic instance.
// Parameters:
//   * name: DTopic name.
//   * concurrency: Maximum number of concurrently processing DTopic messages.
//   * flag: Any flag to control DTopic behaviour.
// Flags for delivery options:
//   * UnorderedDelivery: Messages are delivered in random order. It's good to distribute independent events in a distributed system.
//   * OrderedDelivery: Messages are delivered in order. Not implemented yet.
func (c *Client) NewDTopic(name string, concurrency int, flag int16) (*DTopic, error) {
	if flag&olric.UnorderedDelivery == 0 && flag&olric.OrderedDelivery == 0 {
		return nil, fmt.Errorf("invalid delivery mode: %w", olric.ErrInvalidArgument)
	}
	if flag&olric.OrderedDelivery != 0 {
		return nil, olric.ErrNotImplemented
	}
	return &DTopic{
		Client:      c,
		name:        name,
		flag:        flag,
		concurrency: concurrency,
		listeners:   make(map[uint64]struct{}),
	}, nil
}

// Publish sends a message to the given topic. It accepts any serializable type as message.
func (dt *DTopic) Publish(msg interface{}) error {
	value, err := dt.serializer.Marshal(msg)
	if err != nil {
		return err
	}
	req := protocol.NewDTopicMessage(protocol.OpDTopicPublish)
	req.SetDTopic(dt.name)
	req.SetValue(value)
	resp, err := dt.request(req)
	if err != nil {
		return err
	}
	return checkStatusCode(resp)
}

func (dt *DTopic) listen(l *listener, f func(olric.DTopicMessage)) {
	defer dt.wg.Done()

	// Limit concurrency to prevent CPU or network I/O starvation.
	if dt.concurrency == 0 {
		dt.concurrency = runtime.NumCPU()
	}
	sem := semaphore.NewWeighted(int64(dt.concurrency))
	for {
		select {
		case <-l.ctx.Done():
			// The registered listener is gone.
			return
		case req := <-l.read:
			// A new message is received. Decode it and run the callback function at background.
			err := sem.Acquire(l.ctx, 1)
			if err != nil {
				sem.Release(1)
				logger.Printf("[ERROR] Failed to acquire semaphore: %v\n", err)
				continue
			}
			var msg olric.DTopicMessage
			err = msgpack.Unmarshal(req.Value(), &msg)
			if err != nil {
				sem.Release(1)
				logger.Printf("[ERROR] Failed to unmarshal received message: %v\n", err)
				continue
			}
			// We call the f function in a goroutine to prevent congestion on the stream.
			dt.wg.Add(1)
			go func() {
				defer dt.wg.Done()
				defer sem.Release(1)
				f(msg)
			}()
		}
	}
}

// AddListener adds a new listener for the topic. Returns a listener ID or a non-nil error. The callback functions for this DTopic are run by parallel.
func (dt *DTopic) AddListener(f func(olric.DTopicMessage)) (uint64, error) {
	l := newListener()
	streamID, listenerID, err := dt.addStreamListener(l)
	if err != nil {
		return 0, err
	}

	req := protocol.NewDTopicMessage(protocol.OpDTopicAddListener)
	req.SetDTopic(dt.name)
	req.SetExtra(protocol.DTopicAddListenerExtra{
		ListenerID: listenerID,
		StreamID:   streamID,
	})
	resp, err := dt.request(req)
	if err != nil {
		_ = dt.removeStreamListener(listenerID)
		return 0, err
	}
	err = checkStatusCode(resp)
	if err != nil {
		_ = dt.removeStreamListener(listenerID)
		return 0, err
	}

	// Listen at background. This will call callback function when a message is received.
	dt.wg.Add(1)
	go dt.listen(l, f)

	// This DTopic needs to know its listeners. When the user calls Destroy method, we will call RemoveListener
	// for all of this ids.
	dt.mu.Lock()
	dt.listeners[listenerID] = struct{}{}
	dt.mu.Unlock()
	return listenerID, nil
}

// RemoveListener removes a listener with the given listenerID.
func (dt *DTopic) RemoveListener(listenerID uint64) error {
	// Destroy locally.
	err := dt.removeStreamListener(listenerID)
	if err != nil {
		return err
	}
	dt.mu.Lock()
	delete(dt.listeners, listenerID)
	dt.mu.Unlock()

	// Remove it from the server.
	req := protocol.NewDTopicMessage(protocol.OpDTopicRemoveListener)
	req.SetDTopic(dt.name)
	req.SetExtra(protocol.DTopicRemoveListenerExtra{
		ListenerID: listenerID,
	})
	resp, err := dt.request(req)
	if err != nil {
		return err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return err
	}
	return nil
}

// Destroy a DTopic from the cluster. It stops background goroutines and releases underlying data structures.
func (dt *DTopic) Destroy() error {
	req := protocol.NewDTopicMessage(protocol.OpDTopicDestroy)
	req.SetDTopic(dt.name)
	resp, err := dt.request(req)
	if err != nil {
		return err
	}
	err = checkStatusCode(resp)
	if err != nil {
		return err
	}

	// Remove local listeners
	dt.mu.Lock()
	defer dt.mu.Unlock()
	for listenerID, _ := range dt.listeners {
		err = dt.removeStreamListener(listenerID)
		if err != nil {
			logger.Printf("[ERROR] Failed to remove listener: %d: %v\n", listenerID, err)
			continue
		}
	}
	// I don't know that it's good to remove the map items while iterating over the same map.
	for listenerID, _ := range dt.listeners {
		delete(dt.listeners, listenerID)
	}
	return nil
}
