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

package dset

import (
	"context"
	"errors"
	"sync"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/service"
	"github.com/buraksezer/olric/pkg/flog"
	"github.com/buraksezer/olric/pkg/storage"
)

var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrServerGone      = errors.New("server is gone")
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrUnknownOperation means that an unidentified message has been received from a client.
	ErrUnknownOperation = errors.New("unknown operation")
	ErrNotImplemented   = errors.New("not implemented")
	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")
	errFragmentNotFound = errors.New("fragment not found")
)

type Service struct {
	sync.RWMutex // protects dmaps map

	log        *flog.Logger
	config     *config.Config
	dsets      map[string]*DSet
	operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewService(e *environment.Environment) (service.Service, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		config:     e.Get("config").(*config.Config),
		log:        e.Get("logger").(*flog.Logger),
		dsets:      make(map[string]*DSet),
		operations: make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder)),
		ctx:        ctx,
		cancel:     cancel,
	}
	return s, nil
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

// Starts starts the distributed map service.
func (s *Service) Start() error {
	return nil
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
	case err == ErrKeyNotFound || err == storage.ErrKeyNotFound || err == errFragmentNotFound:
		w.SetStatus(protocol.StatusErrKeyNotFound)
	default:
		w.SetStatus(protocol.StatusInternalServerError)
	}
}

var _ service.Service = (*Service)(nil)
