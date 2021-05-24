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

package transport

import (
	"context"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
	"testing"
)

func TestClient_CreateStream(t *testing.T) {
	s, err := newServer()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	go func() {
		err := s.ListenAndServe()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	defer func() {
		err = s.Shutdown(context.TODO())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	<-s.StartedCtx.Done()

	readCh := make(chan protocol.EncodeDecoder, 1)
	writeCh := make(chan protocol.EncodeDecoder, 1)
	fakeStreamID := uint64(123)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.SetDispatcher(func(_, _ protocol.EncodeDecoder) {
		// this is a mock handler for protocol.OpCreateStream.
		// Handlers for this message doesn't return immediately.
		// The main idea is streaming messages to/from the underlying
		// TCP socket instead of writing a message and returning immediately.
		rq := protocol.NewStreamMessage(protocol.OpStreamCreated)
		rq.SetExtra(protocol.StreamCreatedExtra{
			StreamID: fakeStreamID,
		})
		rq.SetStatus(protocol.StatusOK)
		writeCh <- rq
		// don't return until ctx is cancelled.
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			}
		}
	})

	addr := s.listener.Addr().String()
	// Create a client and make a request. It will never return.
	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	c := NewClient(cc)

	go func() {
		err := c.CreateStream(ctx, addr, readCh, writeCh)
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	msg := <-writeCh
	if msg.Status() != protocol.StatusOK {
		t.Fatalf("Expected Status: %d. Got: %d", protocol.StatusOK, msg.Status())
	}
	extra := msg.Extra().(protocol.StreamCreatedExtra)
	if extra.StreamID != fakeStreamID {
		t.Fatalf("Expected StreamID: %d. Got: %d", fakeStreamID, extra.StreamID)
	}
}
