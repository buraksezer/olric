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
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDTopic_PublishStandalone(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dt, err := db.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg DTopicMessage) {
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
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dt, err := db.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg DTopicMessage) {}
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
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Add listener

	dt, err := db1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var count int32
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg DTopicMessage) {
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

	dt2, err := db2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dt, err := db.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.RemoveListener(1231)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_Destroy(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	dbOne, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dbTwo, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Add listener
	dtOne, err := dbOne.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg DTopicMessage) {}
	listenerID, err := dtOne.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dtTwo, err := dbTwo.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dtTwo.Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dtOne.RemoveListener(listenerID)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_DTopicMessage(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	dbOne, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dbTwo, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Add listener

	dtOne, err := dbOne.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg DTopicMessage) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if msg.PublisherAddr != dbTwo.this.String() {
			t.Fatalf("Expected %s. Got: %s", dbTwo.this.String(), msg.PublisherAddr)
		}

		if msg.PublishedAt <= 0 {
			t.Fatalf("Invalid PublishedAt: %d", msg.PublishedAt)
		}
	}

	listenerID, err := dtOne.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = dtOne.RemoveListener(listenerID)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	// Publish

	dtTwo, err := dbTwo.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Add listener

	dt, err := db1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var count int32
	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg DTopicMessage) {
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected message. Got: %v", err)
		}
		atomic.AddInt32(&count, 1)
		if atomic.LoadInt32(&count) == 10 {
			cancel()
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

	// Publish

	dt2, err := db2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = db.NewDTopic("my-topic", 0, 0)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_OrderedDelivery(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = db.NewDTopic("my-topic", 0, OrderedDelivery)
	if err != ErrNotImplemented {
		t.Errorf("Expected ErrNotImplemented. Got: %v", err)
	}
}
