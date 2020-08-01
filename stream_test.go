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

package olric

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func TestStream_CreateStream(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	streamClosed := make(chan struct{})
	defer func() {
		<-streamClosed
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readCh := make(chan protocol.EncodeDecoder, 1)
	writeCh := make(chan protocol.EncodeDecoder, 1)
	go func() {
		err = db.client.CreateStream(ctx, db.this.String(), readCh, writeCh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		close(streamClosed)
	}()

loop:
	for {
		select {
		case raw := <-readCh:
			msg := raw.(*protocol.StreamMessage)
			if msg.OpCode() != protocol.OpStreamCreated {
				t.Fatalf("Expected OpCode %d: Got: %d", protocol.OpStreamCreated, msg.Op)
			}

			streamID := msg.Extra().(protocol.StreamCreatedExtra).StreamID
			db.streams.mu.RLock()
			_, ok := db.streams.m[streamID]
			db.streams.mu.RUnlock()
			if !ok {
				t.Fatalf("StreamID is invalid: %d", streamID)
			}
			// Everything is OK
			break loop
		case <-time.After(5 * time.Second):
			t.Fatalf("No message received in 5 seconds")
		}
	}
}

func TestStream_EchoMessage(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	streamClosed := make(chan struct{})
	defer func() {
		<-streamClosed
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readCh := make(chan protocol.EncodeDecoder, 1)
	writeCh := make(chan protocol.EncodeDecoder, 1)
	go func() {
		err = db.client.CreateStream(ctx, db.this.String(), readCh, writeCh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		close(streamClosed)
	}()

	f := func(m protocol.EncodeDecoder) {
		streamID := m.Extra().(protocol.StreamCreatedExtra).StreamID
		db.streams.mu.RLock()
		s, _ := db.streams.m[streamID]
		db.streams.mu.RUnlock()

		s.write <- <-s.read
	}

loop:
	for {
		select {
		case raw := <-readCh:
			if raw.OpCode() == protocol.OpStreamCreated {
				msg := raw.(*protocol.StreamMessage)
				go f(msg)
				// Stream is created. Now, we are able to do write or read on this bidirectional channel.
				//
				// Send a test message
				req := protocol.NewDMapMessage(protocol.OpPut)
				req.SetDMap("echo-test-dmap")
				req.SetKey("echo-test-key")
				req.SetValue([]byte("echo-test-value"))
				writeCh <- req
			} else if raw.OpCode() == protocol.OpPut {
				msg := raw.(*protocol.DMapMessage)
				if msg.DMap() != "echo-test-dmap" {
					t.Fatalf("Expected msg.dmap: echo-test-dmap. Got: %s", msg.DMap())
				}
				if msg.Key() != "echo-test-key" {
					t.Fatalf("Expected msg.key: echo-test-key. Got: %s", msg.Key())
				}
				if bytes.Equal(msg.Value(), []byte("echo-test-dmap")) {
					t.Fatalf("Expected msg.value: echo-test-value. Got: %s", string(msg.Value()))
				}
				break loop
			} else {
				t.Fatalf("Invalid message received: %d", raw.OpCode())
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No message received in 5 seconds")
		}
	}
}

func TestStream_PingPong(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	streamClosed := make(chan struct{})
	defer func() {
		<-streamClosed
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	readCh := make(chan protocol.EncodeDecoder, 1)
	writeCh := make(chan protocol.EncodeDecoder, 1)
	go func() {
		err = db.client.CreateStream(ctx, db.this.String(), readCh, writeCh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		close(streamClosed)
	}()

loop:
	for {
		select {
		case raw := <-readCh:
			msg := raw.(*protocol.StreamMessage)
			if msg.OpCode() == protocol.OpStreamCreated {
				writeCh <- protocol.NewStreamMessage(protocol.OpStreamPing)
				continue loop
			}
			if msg.OpCode() != protocol.OpStreamPong {
				t.Fatalf("Expected OpCode %d: Got: %d", protocol.OpStreamPong, msg.Op)
			}
			// Everything is OK
			break loop
		case <-time.After(5 * time.Second):
			t.Fatalf("No message received in 5 seconds")
		}
	}
}
