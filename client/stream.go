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
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

// About the hack: This looks weird, but I need to mock client.CreateStream function to test streams
// independently. I don't want to use a mocking library for this. So I created a function named
// createStreamFunction and I overwrite that function in test.
var createStreamFunction func(context.Context, string, chan<- protocol.EncodeDecoder, <-chan protocol.EncodeDecoder) error

var errTooManyListener = errors.New("stream has too many listeners")

const maxListenersPerStream = 1024

type listener struct {
	read   chan protocol.EncodeDecoder
	write  chan protocol.EncodeDecoder
	ctx    context.Context
	cancel context.CancelFunc
}

func newListener() *listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &listener{
		read:   make(chan protocol.EncodeDecoder, 1),
		write:  make(chan protocol.EncodeDecoder, 1),
		ctx:    ctx,
		cancel: cancel,
	}
}

// streams provides a bidirectional communication channel between Olric nodes and clients. It can also be used
// for node-to-node communication.
type stream struct {
	mu sync.RWMutex

	pongReceivedAt int64
	listeners      map[uint64]*listener
	read           chan protocol.EncodeDecoder
	write          chan protocol.EncodeDecoder
	errCh          chan error
	ctx            context.Context
	cancel         context.CancelFunc
}

// streams maps StreamIDs to streams
type streams struct {
	mu sync.RWMutex

	m map[uint64]*stream
}

func (s *stream) listenStream() {
	for {
		select {
		case msg := <-s.read:
			s.mu.RLock()
			for id, l := range s.listeners {
				if msg.OpCode() == protocol.OpStreamPong {
					atomic.StoreInt64(&s.pongReceivedAt, time.Now().UnixNano())
					continue
				}
				if msg.OpCode() != protocol.OpStreamMessage {
					logger.Printf("[ERROR] Received message is not a stream message: %d\n", msg.OpCode())
					continue
				}
				if msg.Extra().(protocol.StreamMessageExtra).ListenerID == id {
					l.read <- msg
				}
			}
			s.mu.RUnlock()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *stream) checkStreamAliveness() {
loop:
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Second):
			s.mu.RLock()
			pongReceivedAt := atomic.LoadInt64(&s.pongReceivedAt)
			if pongReceivedAt == 0 {
				s.mu.RUnlock()
				continue loop
			}
			if pongReceivedAt+(5*time.Second).Nanoseconds() <= time.Now().UnixNano() {
				// There is no need to call stream.close method here. The underlying socket is already gone.
				s.cancel()
				logger.Print("[WARN] Stream is dead. Closing.\n")
			}
			s.mu.RUnlock()
		}
	}
}

func (s *stream) sendPingMessage() {
	for {
		select {
		case <-time.After(time.Second):
			s.write <- protocol.NewStreamMessage(protocol.OpStreamPing)
		case <-s.ctx.Done():
			return
		}
	}
}

func (c *Client) createStream() (uint64, *stream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &stream{
		listeners: make(map[uint64]*listener),
		read:      make(chan protocol.EncodeDecoder, 1),
		write:     make(chan protocol.EncodeDecoder, 1),
		errCh:     make(chan error, 1),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Pick a random addr to dial
	if len(c.config.Servers) == 0 {
		return 0, nil, fmt.Errorf("no addr found to dial")
	}
	idx := rand.Intn(len(c.config.Servers))
	addr := c.config.Servers[idx]

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		s.errCh <- createStreamFunction(ctx, addr, s.read, s.write)
	}()

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case err := <-s.errCh:
		return 0, nil, err
	case raw := <-s.read:
		msg := raw.(*protocol.StreamMessage)
		if raw.OpCode() != protocol.OpStreamCreated {
			return 0, nil, fmt.Errorf("server returned OpCode: %d instead of %d", msg.OpCode(), protocol.OpStreamCreated)
		}

		streamID := msg.Extra().(protocol.StreamCreatedExtra).StreamID
		c.streams.m[streamID] = s

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			s.listenStream()
		}()

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			s.sendPingMessage()
		}()

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			s.checkStreamAliveness()
		}()

		return streamID, s, nil
	case <-timer.C:
		return 0, nil, fmt.Errorf("streamID could not be retrieved")
	}
}

func (c *Client) writeToStream(s *stream, l *listener) {
	defer c.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-l.ctx.Done():
			return
		case msg := <-l.write:
			s.write <- msg
		}
	}
}

func (c *Client) addStreamListener(l *listener) (uint64, uint64, error) {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()

	add := func(s *stream) (uint64, error) {
		s.mu.Lock()
		defer s.mu.Unlock()

		if len(s.listeners) >= c.config.MaxListenersPerStream {
			return 0, errTooManyListener
		}
		listenerID := rand.Uint64()
		s.listeners[listenerID] = l

		c.wg.Add(1)
		go c.writeToStream(s, l)

		return listenerID, nil
	}

	for streamID, s := range c.streams.m {
		listenerID, err := add(s)
		if err == errTooManyListener {
			continue
		}
		return streamID, listenerID, err
	}

	streamID, s, err := c.createStream()
	if err != nil {
		return 0, 0, err
	}
	listenerID, err := add(s)
	if err != nil {
		return 0, 0, err
	}
	return streamID, listenerID, nil
}

func (c *Client) removeStreamListener(listenerID uint64) error {
	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()

	for _, s := range c.streams.m {
		for id, l := range s.listeners {
			if id == listenerID {
				l.cancel() // this closes underlying goroutines
				delete(s.listeners, id)
				return nil
			}
		}
	}
	return fmt.Errorf("no listener found with given ID")
}
