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

package dtopic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
)

func TestDTopic_PublishStandalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dt, err := s.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg Message) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dt.RemoveListener(listenerID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	err = dt.Publish("message")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Failed to call onMessage function")
	}
}

func TestDTopic_RemoveListener(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dt, err := s.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg Message) {}
	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.RemoveListener(listenerID)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDTopic_PublishCluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Add listener

	dt, err := s1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var count int32
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg Message) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		atomic.AddInt32(&count, 1)
	}

	listenerID, err := dt.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dt.RemoveListener(listenerID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	// Publish

	dt2, err := s2.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dt2.Publish("message")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	select {
	case <-ctx.Done():
		if atomic.LoadInt32(&count) != 1 {
			t.Fatalf("Expected count 1. Got: %d", atomic.LoadInt32(&count))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Failed to call onMessage function")
	}
}

func TestDTopic_RemoveListenerNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dt, err := s.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.RemoveListener(1231)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_Destroy(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Add listener
	dt1, err := s1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg Message) {}
	listenerID, err := dt1.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dt2, err := s2.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dt2.Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dt1.RemoveListener(listenerID)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_DTopicMessage(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Add listener

	dt1, err := s1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg Message) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if msg.PublisherAddr != s2.rt.This().String() {
			t.Fatalf("Expected %s. Got: %s", s2.rt.This().String(), msg.PublisherAddr)
		}

		if msg.PublishedAt <= 0 {
			t.Fatalf("Invalid PublishedAt: %d", msg.PublishedAt)
		}
	}

	listenerID, err := dt1.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dt1.RemoveListener(listenerID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	// Publish

	dtTwo, err := s2.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dtTwo.Publish("message")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Failed to call onMessage function")
	}
}

func TestDTopic_PublishMessagesCluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Add listener

	dt1, err := s1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var count int32
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg Message) {
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected message. Got: %v", err)
		}
		atomic.AddInt32(&count, 1)
		if atomic.LoadInt32(&count) == 10 {
			cancel()
		}
	}

	listenerID, err := dt1.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dt1.RemoveListener(listenerID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	// Publish

	dt2, err := s2.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dt2.Publish("message")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	select {
	case <-ctx.Done():
		if atomic.LoadInt32(&count) != 10 {
			t.Fatalf("Expected count 10. Got: %d", atomic.LoadInt32(&count))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Failed to call onMessage function")
	}
}

func TestDTopic_DeliveryOrder(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	_, err := s.NewDTopic("my-topic", 0, 0)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_OrderedDelivery(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	_, err := s.NewDTopic("my-topic", 0, OrderedDelivery)
	if err != ErrNotImplemented {
		t.Errorf("Expected ErrNotImplemented. Got: %v", err)
	}
}
