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

package olric

import (
	"errors"

	"github.com/buraksezer/olric/internal/dtopic"
	"github.com/buraksezer/olric/pkg/neterrors"
)

const (
	// UnorderedDelivery means that messages are delivered in random order.
	// It's good to distribute independent events in a distributed system.
	UnorderedDelivery = int16(1) << iota

	// OrderedDelivery means that messages are delivered in some order.
	// Not implemented yet.
	OrderedDelivery
)

// DTopicMessage is a message type for DTopic data structure.
type DTopicMessage struct {
	Message       interface{}
	PublisherAddr string
	PublishedAt   int64
}

type DTopic struct {
	dt *dtopic.DTopic
}

func convertDTopicError(err error) error {
	switch {
	case errors.Is(err, neterrors.ErrInvalidArgument):
		return ErrInvalidArgument
	case errors.Is(err, neterrors.ErrNotImplemented):
		return ErrNotImplemented
	case errors.Is(err, neterrors.ErrOperationTimeout):
		return ErrOperationTimeout
	case errors.Is(err, neterrors.ErrUnknownOperation):
		return ErrUnknownOperation
	default:
		return err
	}
}

// NewDTopic returns a new distributed topic instance.
// Parameters:
//   * name: DTopic name.
//   * concurrency: Maximum number of concurrently processing DTopic messages.
//   * flag: Any flag to control DTopic behavior.
// Flags for delivery options:
//   * UnorderedDelivery: Messages are delivered in random order. It's good to distribute independent events in a distributed system.
//   * OrderedDelivery: Messages are delivered in order. Not implemented yet.
func (db *Olric) NewDTopic(name string, concurrency int, flag int16) (*DTopic, error) {
	dt, err := db.services.dtopic.NewDTopic(name, concurrency, flag)
	if err != nil {
		return nil, convertDTopicError(err)
	}
	return &DTopic{
		dt: dt,
	}, nil
}

func (dt *DTopic) Publish(msg interface{}) error {
	err := dt.dt.Publish(msg)
	return convertDTopicError(err)
}

func (dt *DTopic) AddListener(f func(DTopicMessage)) (uint64, error) {
	listenerID, err := dt.dt.AddListener(func(msg dtopic.Message) {
		f(DTopicMessage(msg))
	})
	if err != nil {
		return 0, convertDTopicError(err)
	}
	return listenerID, nil
}

func (dt *DTopic) RemoveListener(listenerID uint64) error {
	err := dt.dt.RemoveListener(listenerID)
	return convertDTopicError(err)
}

func (dt *DTopic) Destroy() error {
	err := dt.dt.Destroy()
	return convertDTopicError(err)
}
