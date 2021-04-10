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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/testolric"
)

func TestClient_DTopicPublish(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.Publish("my-message")
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
}

func TestClient_DTopicPublishMessages(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	var counter int32

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(message olric.DTopicMessage) {
		atomic.AddInt32(&counter, 1)
		if atomic.LoadInt32(&counter) == 10 {
			cancel()
		}
	}

	_, err = dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	for i := 0; i < 10; i++ {
		err = dt.Publish("my-message")
		if err != nil {
			t.Errorf("Expected nil. Got: %s", err)
		}
	}

	select {
	case <-ctx.Done():
		c := atomic.LoadInt32(&counter)
		if c != 10 {
			t.Fatalf("Expected message count: 10. Got: %d", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout exceeded")
	}
}

func TestClient_DTopicAddListener(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
}

func TestClient_DTopicOnMessage(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg olric.DTopicMessage) {
		defer cancel()
		if msg.Message != "my-message" {
			t.Fatalf("Expected my-message. Got: %s", msg.Message)
		}
		if msg.PublishedAt <= 0 {
			t.Fatalf("Invalid published at: %d", msg.PublishedAt)
		}
	}

	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	err = dt.Publish("my-message")
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("No message received in 5 seconds")
	}
}

func TestClient_DTopicRemoveListener(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	err = dt.RemoveListener(listenerID)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	dt.streams.mu.RLock()
	defer dt.streams.mu.RUnlock()

	for _, s := range dt.streams.m {
		for id, _ := range s.listeners {
			if id == listenerID {
				t.Fatalf("ListenerID: %d is still exist", id)
			}
		}
	}
}

func TestClient_DTopicDestroy(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	onMessage := func(message olric.DTopicMessage) {}
	dt, err := c.NewDTopic("my-dtopic", 0, olric.UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	err = dt.Destroy()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	dt.streams.mu.RLock()
	defer dt.streams.mu.RUnlock()

	for _, s := range dt.streams.m {
		for id, _ := range s.listeners {
			if id == listenerID {
				t.Fatalf("ListenerID: %d is still exist", id)
			}
		}
	}
}

func TestDTopic_DeliveryOrder(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = c.NewDTopic("my-topic", 0, 0)
	if !errors.Is(err, olric.ErrInvalidArgument) {
		t.Errorf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_OrderedDelivery(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = c.NewDTopic("my-topic", 0, olric.OrderedDelivery)
	if err != olric.ErrNotImplemented {
		t.Errorf("Expected ErrNotImplemented. Got: %v", err)
	}
}
