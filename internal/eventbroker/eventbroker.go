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

package eventbroker

import (
	"errors"
	"sync"

	"github.com/buraksezer/olric/events"
	"github.com/buraksezer/olric/internal/dtopic"
)

var ErrEventSourceNotFound = errors.New("event source not found")

type EventSource struct {
	listenerID uint64
	dtopic     *dtopic.DTopic
}

func (es *EventSource) Publish(msg interface{}) error {
	return es.dtopic.Publish(msg)
}

type EventBroker struct {
	mu sync.RWMutex

	source  map[string]*EventSource
	service *dtopic.Service
}

func New(service *dtopic.Service) *EventBroker {
	return &EventBroker{
		source:  make(map[string]*EventSource),
		service: service,
	}
}

func (e *EventBroker) Register(name string, f func(e events.Event)) error {
	dt, err := e.service.NewDTopic(name, 50, dtopic.UnorderedDelivery)
	if err != nil {
		return err
	}

	listenerID, err := dt.AddListener(func(message dtopic.Message) {
		f(events.Event(message))
	})
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	es := &EventSource{
		listenerID: listenerID,
		dtopic:     dt,
	}
	e.source[name] = es

	return nil
}

func (e *EventBroker) unregister(name string) error {
	es, ok := e.source[name]
	if !ok {
		return ErrEventSourceNotFound
	}

	err := es.dtopic.RemoveListener(es.listenerID)
	if err != nil {
		return err
	}

	err = es.dtopic.Destroy()
	if err != nil {
		return err
	}

	delete(e.source, name)
	return nil
}

func (e *EventBroker) Unregister(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.unregister(name)
}

func (e *EventBroker) GetEventSource(name string) (*EventSource, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	es, ok := e.source[name]
	if !ok {
		return nil, ErrEventSourceNotFound
	}
	return es, nil
}

func (e *EventBroker) Shutdown() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for name := range e.source {
		err := e.unregister(name)
		if err != nil {
			return err
		}

		delete(e.source, name)
	}

	return nil
}
