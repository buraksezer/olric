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
	"errors"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/internal/streams"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/serializer"
)

var ErrServerGone = errors.New("server is gone")

type Service struct {
	sync.RWMutex

	log        *flog.Logger
	serializer serializer.Serializer
	client     *transport.Client
	rt         *routingtable.RoutingTable
	streams    *streams.Streams
	dispatcher *Dispatcher
	m          map[string]*DTopic
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		streams:    e.Get("streams").(*streams.Streams),
		serializer: e.Get("config").(*config.Config).Serializer,
		client:     e.Get("client").(*transport.Client),
		log:        e.Get("logger").(*flog.Logger),
		rt:         e.Get("routingtable").(*routingtable.RoutingTable),
		dispatcher: NewDispatcher(context.Background()),
		m:          make(map[string]*DTopic),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

func (s *Service) Start() error {
	// dummy implementation
	return nil
}

func (s *Service) isAlive() bool {
	select {
	case <-s.ctx.Done():
		// The node is gone.
		return false
	default:
	}
	return true
}

func (s *Service) unmarshalValue(raw []byte) (interface{}, error) {
	var value interface{}
	err := s.serializer.Unmarshal(raw, &value)
	if err != nil {
		return nil, err
	}
	if _, ok := value.(struct{}); ok {
		return nil, nil
	}
	return value, nil
}

func (s *Service) requestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	resp, err := s.client.RequestTo(addr, req)
	if err != nil {
		return nil, err
	}
	status := resp.Status()
	if status == protocol.StatusOK {
		return resp, nil
	}

	switch status {
	case protocol.StatusErrInternalFailure:
		return nil, neterrors.Wrap(neterrors.ErrInternalFailure, string(resp.Value()))
	case protocol.StatusErrInvalidArgument:
		return nil, neterrors.Wrap(neterrors.ErrInvalidArgument, string(resp.Value()))
	}
	return nil, neterrors.GetByCode(status)
}

func (s *Service) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
	// Operations on DTopic data structure
	//
	// DTopic.Publish
	operations[protocol.OpPublishDTopicMessage] = s.publishMessageOperation
	operations[protocol.OpDTopicPublish] = s.exPublishOperation

	// DTopic.Destroy
	operations[protocol.OpDestroyDTopicInternal] = s.destroyDTopicInternalOperation
	operations[protocol.OpDTopicDestroy] = s.dtopicDestroyOperation

	// DTopic.AddListener
	operations[protocol.OpDTopicAddListener] = s.addListenerOperation

	// DTopic.RemoveListener
	operations[protocol.OpDTopicRemoveListener] = s.removeListenerOperation
}

func (s *Service) Shutdown(ctx context.Context) error {
	s.cancel()
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
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

var _ service.Service = (*Service)(nil)
