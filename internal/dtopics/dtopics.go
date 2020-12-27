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
	"context"
	"errors"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/streams"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/serializer"
)

var ErrServerGone = errors.New("server is gone")

type DTopics struct {
	sync.RWMutex

	log        *flog.Logger
	serializer serializer.Serializer
	client     *transport.Client
	rt         *routing_table.RoutingTable
	streams    *streams.Streams
	dispatcher *Dispatcher
	m          map[string]*DTopic
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func New(e *environment.Environment, s *streams.Streams) *DTopics {
	ctx, cancel := context.WithCancel(context.Background())
	return &DTopics{
		streams:    s,
		serializer: e.Get("config").(*config.Config).Serializer,
		client:     e.Get("client").(*transport.Client),
		log:        e.Get("logger").(*flog.Logger),
		rt:         e.Get("routingTable").(*routing_table.RoutingTable),
		dispatcher: NewDispatcher(context.Background()),
		m:          make(map[string]*DTopic),
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (ds *DTopics) isAlive() bool {
	select {
	case <-ds.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

func (ds *DTopics) unmarshalValue(raw []byte) (interface{}, error) {
	var value interface{}
	err := ds.serializer.Unmarshal(raw, &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

func (ds *DTopics) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
	// Operations on DTopic data structure
	//
	// DTopic.Publish
	operations[protocol.OpPublishDTopicMessage] = ds.publishMessageOperation
	operations[protocol.OpDTopicPublish] = ds.exPublishOperation

	// DTopic.Destroy
	operations[protocol.OpDestroyDTopic] = ds.destroyOperation
	operations[protocol.OpDTopicDestroy] = ds.exDestroyOperation

	// DTopic.AddListener
	operations[protocol.OpDTopicAddListener] = ds.exAddListenerOperation

	// DTopic.RemoveListener
	operations[protocol.OpDTopicRemoveListener] = ds.exRemoveListenerOperation
}

func (ds *DTopics) Shutdown(ctx context.Context) error {
	ds.cancel()
	done := make(chan struct{})

	go func() {
		ds.wg.Wait()
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

func errorResponse(w protocol.EncodeDecoder, err error) {
	getError := func(err interface{}) []byte {
		switch val := err.(type) {
		case string:
			return []byte(val)
		case error:
			return []byte(val.Error())
		default:
			return nil
		}
	}
	w.SetValue(getError(err))

	switch {
	case err == ErrOperationTimeout, errors.Is(err, ErrOperationTimeout):
		w.SetStatus(protocol.StatusErrOperationTimeout)
	case err == routing_table.ErrClusterQuorum, errors.Is(err, routing_table.ErrClusterQuorum):
		w.SetStatus(protocol.StatusErrClusterQuorum)
	case err == ErrUnknownOperation, errors.Is(err, ErrUnknownOperation):
		w.SetStatus(protocol.StatusErrUnknownOperation)
	case err == ErrServerGone, errors.Is(err, ErrServerGone):
		w.SetStatus(protocol.StatusErrServerGone)
	case err == ErrInvalidArgument, errors.Is(err, ErrInvalidArgument):
		w.SetStatus(protocol.StatusErrInvalidArgument)
	case err == ErrNotImplemented, errors.Is(err, ErrNotImplemented):
		w.SetStatus(protocol.StatusErrNotImplemented)
	default:
		w.SetStatus(protocol.StatusInternalServerError)
	}
}
