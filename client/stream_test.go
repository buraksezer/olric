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

package client

import (
	"bytes"
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
)

func mockCreateStream(ctx context.Context, _ string, read chan<- protocol.EncodeDecoder, write <-chan protocol.EncodeDecoder) error {
	rq := protocol.NewStreamMessage(protocol.OpStreamCreated)
	rq.SetExtra(protocol.StreamCreatedExtra{
		StreamID: rand.Uint64(),
	})
	read <- rq
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-write:
			if msg.OpCode() == protocol.OpStreamPing {
				read <- protocol.NewStreamMessage(protocol.OpStreamPong)
			} else {
				read <- msg
			}
		}
	}
}

func TestStream_EchoListener(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	l := newListener()
	_, listenerID, err := c.addStreamListener(l)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	buf := new(bytes.Buffer)
	req := protocol.NewDMapMessage(protocol.OpPut)
	req.SetBuffer(buf)
	req.SetDMap("mydmap")
	req.SetKey("mykey")
	req.SetValue([]byte("myvalue"))
	err = req.Encode()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	str := protocol.ConvertToStreamMessage(req, listenerID)
	l.write <- str

	select {
	case raw := <-l.read:
		conn := protocol.NewBytesToConn(raw.Value())
		buf.Reset()
		header, err := protocol.ReadMessage(conn, buf)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if header.Magic != protocol.MagicDMapReq {
			t.Fatalf("Expected protocol.MagicDMapReq (%d). Got %d", protocol.MagicDMapReq, header.Magic)
		}
		req = protocol.NewDMapMessageFromRequest(buf)
		err = req.Decode()
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if req.DMap() != "mydmap" {
			t.Fatalf("Expected dmap: %s. Got: %s", req.DMap(), req.DMap())
		}
		if req.Key() != "mykey" {
			t.Fatalf("Expected key: %s. Got: %s", req.Key(), req.Key())
		}
		if !bytes.Equal(req.Value(), []byte("myvalue")) {
			t.Fatalf("Expected value: %s. Got: %s", string(req.Value()), req.Value())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("No message received from listener")
	}
}

func TestStream_CreateNewStream(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()
	modifiedTestConfig := *testConfig
	modifiedTestConfig.MaxListenersPerStream = 1
	c, err := New(&modifiedTestConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	l1 := newListener()
	_, _, err = c.addStreamListener(l1)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	l2 := newListener()
	_, _, err = c.addStreamListener(l2)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	c.streams.mu.RLock()
	defer c.streams.mu.RUnlock()
	if len(c.streams.m) != 2 {
		t.Fatalf("Expected stream count is 2. Got: %d", len(c.streams.m))
	}
}

func TestStream_MultipleListeners(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()
	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	listeners := make(map[uint64]*listener)

	l1 := newListener()
	_, listenerID1, err := c.addStreamListener(l1)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	listeners[listenerID1] = l1

	l2 := newListener()
	_, listenerID2, err := c.addStreamListener(l2)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	listeners[listenerID2] = l2

	c.streams.mu.RLock()
	defer c.streams.mu.RUnlock()
	if len(c.streams.m) != 1 {
		t.Fatalf("Expected stream count is 1. Got: %d", len(c.streams.m))
	}

	for id, l := range listeners {
		buf := new(bytes.Buffer)
		msg := protocol.NewDMapMessage(protocol.OpPut)
		msg.SetBuffer(buf)
		msg.SetDMap("mydmap")
		msg.SetKey("mykey")
		msg.SetValue([]byte("myvalue"))
		if err = msg.Encode(); err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		req := protocol.ConvertToStreamMessage(msg, id)
		l.write <- req
		select {
		case raw := <-l.read:
			buf.Reset()
			conn := protocol.NewBytesToConn(raw.Value())
			header, err := protocol.ReadMessage(conn, buf)
			if err != nil {
				t.Fatalf("Expected nil. Got %v", err)
			}
			if header.Magic != protocol.MagicDMapReq {
				t.Fatalf("Expected protocol.MagicDMapReq (%d). Got %d", protocol.MagicDMapReq, header.Magic)
			}

			msg := protocol.NewDMapMessageFromRequest(buf)
			err = msg.Decode()
			if err != nil {
				t.Fatalf("Expected nil. Got %v", err)
			}

			if msg.DMap() != "mydmap" {
				t.Fatalf("Expected dmap: mydmap. Got: %s", msg.DMap())
			}
			if msg.Key() != "mykey" {
				t.Fatalf("Expected key: mykey. Got: %s", msg.Key())
			}
			if !bytes.Equal(msg.Value(), []byte("myvalue")) {
				t.Fatalf("Expected value: %s. Got: %s", string(req.Value()), msg.Value())
			}
			listenerID := raw.Extra().(protocol.StreamMessageExtra).ListenerID
			if listenerID != id {
				t.Fatalf("Expected ListenerID: %d. Got: %d", id, listenerID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("No message received from listener")
		}
	}
}

func TestStream_PingPong(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Fatalf("Expected nil. Got %v", serr)
		}
		<-done
	}()
	modifiedTestConfig := *testConfig
	modifiedTestConfig.MaxListenersPerStream = 1
	c, err := New(&modifiedTestConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Mock transport.Client.CreateStream
	createStreamFunction = mockCreateStream

	l := newListener()
	streamID, _, err := c.addStreamListener(l)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	<-time.After(3 * time.Second)

	c.streams.mu.Lock()
	defer c.streams.mu.Unlock()
	s, ok := c.streams.m[streamID]
	if !ok {
		t.Fatalf("StreamID: %d count not be found", streamID)
	}
	if s.pongReceivedAt == 0 {
		t.Fatalf("Expected pongReceivedAt different than zero. Got %d", s.pongReceivedAt)
	}
}
