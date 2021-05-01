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

package dtopic

import (
	"context"
	"fmt"
	"github.com/buraksezer/olric/pkg/neterrors"
	"math/rand"
	"runtime"
	"sync"

	"golang.org/x/sync/semaphore"
)

type listener struct {
	f func(message Message)
}

// Listeners with id.
type listeners struct {
	m           map[uint64]*listener
	concurrency int
}

type Dispatcher struct {
	sync.RWMutex

	m   map[string]*listeners
	ctx context.Context
}

func NewDispatcher(ctx context.Context) *Dispatcher {
	return &Dispatcher{
		m:   make(map[string]*listeners),
		ctx: ctx,
	}
}

func (d *Dispatcher) _addListener(listenerID uint64, topic string, concurrency int, f func(Message)) error {
	d.Lock()
	defer d.Unlock()

	l, ok := d.m[topic]
	if ok {
		if _, ok = l.m[listenerID]; ok {
			return errListenerIDCollision
		}
		l.m[listenerID] = &listener{f: f}
	} else {
		d.m[topic] = &listeners{
			m:           make(map[uint64]*listener),
			concurrency: concurrency,
		}
		d.m[topic].m[listenerID] = &listener{f: f}
	}
	return nil
}

func (d *Dispatcher) addListener(topic string, concurrency int, f func(Message)) (uint64, error) {
	listenerID := rand.Uint64()
	err := d._addListener(listenerID, topic, concurrency, f)
	if err != nil {
		return 0, err
	}
	return listenerID, nil
}

// addRemoveListener registers clients listeners.
func (d *Dispatcher) addRemoteListener(listenerID uint64, topic string, concurrency int, f func(Message)) error {
	return d._addListener(listenerID, topic, concurrency, f)
}

func (d *Dispatcher) removeListener(topic string, listenerID uint64) error {
	d.Lock()
	defer d.Unlock()

	l, ok := d.m[topic]
	if !ok {
		return neterrors.Wrap(neterrors.ErrInvalidArgument, fmt.Sprintf("topic not found: %s", topic))
	}

	_, ok = l.m[listenerID]
	if !ok {
		return neterrors.Wrap(neterrors.ErrInvalidArgument, fmt.Sprintf("listener not found: %d", listenerID))
	}

	delete(l.m, listenerID)
	if len(l.m) == 0 {
		delete(d.m, topic)
	}
	return nil
}

// dispatch sends a received message to all registered listeners. It doesn't store
// any message. It just dispatch the message.
func (d *Dispatcher) dispatch(topic string, msg *Message) error {
	d.RLock()
	defer d.RUnlock()

	l, ok := d.m[topic]
	if !ok {
		// there is no listener for this topic on this node.
		return fmt.Errorf("%w: topic not found: %s", neterrors.ErrInvalidArgument, topic)
	}

	var wg sync.WaitGroup
	if l.concurrency == 0 {
		// the number of logical CPUs usable by the current process.
		l.concurrency = runtime.NumCPU()
	}
	sem := semaphore.NewWeighted(int64(l.concurrency))
	for _, ll := range l.m {
		if err := sem.Acquire(d.ctx, 1); err != nil {
			return err
		}

		wg.Add(1)
		// Dereference the pointer and make a copy of DTopicMessage for every listener.
		go func(f func(message Message)) {
			defer wg.Done()
			defer sem.Release(1)
			f(*msg)
		}(ll.f)
	}
	wg.Wait()
	return nil
}

func (d *Dispatcher) destroy(topic string) {
	d.Lock()
	defer d.Unlock()
	delete(d.m, topic)
}
