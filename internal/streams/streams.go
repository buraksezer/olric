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

package streams

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/flog"
)

var ErrStreamNotFound = errors.New("stream could not be found")

// pool is good for recycling memory while reading messages from the socket.
var bufferPool = bufpool.New()

// Streams maps StreamIDs to Stream
type Streams struct {
	sync.RWMutex

	log    *flog.Logger
	m      map[uint64]*Stream
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func New(e *environment.Environment) *Streams {
	log := e.Get("logger").(*flog.Logger)
	ctx, cancel := context.WithCancel(context.Background())
	return &Streams{
		log:    log,
		m:      make(map[uint64]*Stream),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (ss *Streams) RegisterOperations(operations map[protocol.OpCode]func(w, r protocol.EncodeDecoder)) {
	operations[protocol.OpCreateStream] = ss.createStreamOperation
}

func (ss *Streams) Shutdown(ctx context.Context) error {
	ss.RLock()
	defer ss.RUnlock()

	for _, stream := range ss.m {
		stream.Close()
	}

	done := make(chan struct{})
	go func() {
		ss.wg.Wait()
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

func (ss *Streams) GetStreamByID(id uint64) (*Stream, error) {
	ss.RLock()
	defer ss.RUnlock()

	s, ok := ss.m[id]
	if !ok {
		return nil, ErrStreamNotFound
	}
	return s, nil
}

func (ss *Streams) DeleteStreamByID(id uint64) {
	ss.Lock()
	defer ss.Unlock()

	delete(ss.m, id)
}

func (ss *Streams) checkStreamAliveness(s *Stream, streamID uint64) {
	defer ss.wg.Done()

loop:
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ss.ctx.Done():
			return
		case <-time.After(time.Second):
			s.mu.RLock()
			if s.pingReceivedAt == 0 {
				s.mu.RUnlock()
				continue loop
			}
			if s.pingReceivedAt+(5*time.Second).Nanoseconds() <= time.Now().UnixNano() {
				// There is no need to call Stream.Close method here. The underlying socket is already gone.
				s.cancel()
				ss.log.V(4).Printf("[INFO] StreamID: %d is dead", streamID)
			}
			s.mu.RUnlock()
		}
	}
}

func (ss *Streams) createStreamOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.StreamMessage)

	streamID := rand.Uint64()
	ctx, cancel := context.WithCancel(context.Background())
	ss.Lock()
	s := &Stream{
		conn:   req.Conn(),
		read:   make(chan protocol.EncodeDecoder, 1),
		write:  make(chan protocol.EncodeDecoder, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	// this cancel function will be called by the server when the underlying socket is gone.
	req.SetCancelFunc(s.cancel)
	ss.m[streamID] = s
	ss.Unlock()

	ss.wg.Add(1)
	go ss.checkStreamAliveness(s, streamID)

	defer func() {
		ss.DeleteStreamByID(streamID)
		ss.log.V(4).Printf("[INFO] StreamID: %d is gone", streamID)
	}()

	rq := protocol.NewStreamMessage(protocol.OpStreamCreated)
	rq.SetExtra(protocol.StreamCreatedExtra{
		StreamID: streamID,
	})
	rq.SetStatus(protocol.StatusOK)
	s.write <- rq

	s.errGr.Go(func() error {
		return s.writeLoop()
	})

	s.errGr.Go(func() error {
		return s.readLoop()
	})

loop:
	for {
		select {
		case <-s.ctx.Done():
			// Stream.Close method is called
			break loop
		case <-ss.ctx.Done():
			// server is gone
			break loop
		}
	}

	// this closes the Stream goroutines
	s.cancel()
	if err := s.conn.Close(); err != nil {
		ss.log.V(4).Printf("Failed to Close underlying TCP socket of StreamID: %d", streamID)
	}

	if err := s.errGr.Wait(); err != nil {
		w.SetStatus(protocol.StatusErrInternalFailure)
		w.SetValue([]byte(err.Error()))
		return
	}
	w.SetStatus(protocol.StatusOK)
}
