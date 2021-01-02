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
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/cluster/routing_table"
	"github.com/buraksezer/olric/internal/environment"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/streams"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/transport"
	"golang.org/x/sync/errgroup"
)

func newTestEnvironment(c *config.Config) *environment.Environment {
	if c == nil {
		c = testutil.NewConfig()
	}

	e := environment.New()
	e.Set("config", c)
	e.Set("logger", testutil.NewFlogger(c))
	e.Set("client", transport.NewClient(c.Client))
	e.Set("primary", partitions.New(c.PartitionCount, partitions.PRIMARY))
	e.Set("backup", partitions.New(c.PartitionCount, partitions.BACKUP))
	return e
}

func newDTopicsForTest(e *environment.Environment, srv *transport.Server) *Service {
	rt := routing_table.New(e)
	ops := make(map[protocol.OpCode]func(w, r protocol.EncodeDecoder))
	rt.RegisterOperations(ops)
	requestDispatcher := func(w, r protocol.EncodeDecoder) {
		f := ops[r.OpCode()]
		f(w, r)
	}
	srv.SetDispatcher(requestDispatcher)
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			panic(fmt.Sprintf("ListenAndServe returned an error: %v", err))
		}
	}()
	<-srv.StartedCtx.Done()

	e.Set("routingTable", rt)

	ss := streams.New(e)
	ds := NewService(e, ss)
	ds.RegisterOperations(ops)
	return ds
}

type testCluster struct {
	peerPorts []int
	errGr     errgroup.Group
	ctx       context.Context
	cancel    context.CancelFunc
}

func newTestCluster() *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	return &testCluster{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (t *testCluster) addNode(e *environment.Environment) (*Service, error) {
	if e == nil {
		e = newTestEnvironment(nil)
	}
	c := e.Get("config").(*config.Config)

	port, err := testutil.GetFreePort()
	if err != nil {
		return nil, err
	}
	c.MemberlistConfig.BindPort = port

	var peers []string
	for _, peerPort := range t.peerPorts {
		peers = append(peers, net.JoinHostPort("127.0.0.1", strconv.Itoa(peerPort)))
	}
	c.Peers = peers

	srv := testutil.NewTransportServer(c)
	ds := newDTopicsForTest(e, srv)
	err = ds.rt.Start()
	if err != nil {
		return nil, err
	}

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return srv.Shutdown(context.Background())
	})

	t.errGr.Go(func() error {
		<-t.ctx.Done()
		return ds.rt.Shutdown(context.Background())
	})

	t.peerPorts = append(t.peerPorts, port)
	return ds, err
}

func (t *testCluster) shutdown() error {
	t.cancel()
	return t.errGr.Wait()
}

func TestDTopic_PublishStandalone(t *testing.T) {
	cluster := newTestCluster()
	e := newTestEnvironment(nil)
	ds, err := cluster.addNode(e)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = ds.Shutdown(context.Background())
		if err != nil {
			ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
		}
	}()

	dt, err := ds.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e := newTestEnvironment(nil)
	ds, err := cluster.addNode(e)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = ds.Shutdown(context.Background())
		if err != nil {
			ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
		}
	}()

	dt, err := ds.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e1 := newTestEnvironment(nil)
	ds1, err := cluster.addNode(e1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	e2 := newTestEnvironment(nil)
	ds2, err := cluster.addNode(e2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		for _, ds := range []*Service{ds1, ds2} {
			err = ds.Shutdown(context.Background())
			if err != nil {
				ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
			}
		}
	}()

	// Add listener

	dt, err := ds1.NewDTopic("my-topic", 0, UnorderedDelivery)
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

	dt2, err := ds2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e := newTestEnvironment(nil)
	ds, err := cluster.addNode(e)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = ds.Shutdown(context.Background())
		if err != nil {
			ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
		}
	}()

	dt, err := ds.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dt.RemoveListener(1231)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_Destroy(t *testing.T) {
	cluster := newTestCluster()
	e1 := newTestEnvironment(nil)
	ds1, err := cluster.addNode(e1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	e2 := newTestEnvironment(nil)
	ds2, err := cluster.addNode(e2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		for _, ds := range []*Service{ds1, ds2} {
			err = ds.Shutdown(context.Background())
			if err != nil {
				ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
			}
		}
	}()

	// Add listener
	dt1, err := ds1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	onMessage := func(msg Message) {}
	listenerID, err := dt1.AddListener(onMessage)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dt2, err := ds2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e1 := newTestEnvironment(nil)
	ds1, err := cluster.addNode(e1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	e2 := newTestEnvironment(nil)
	ds2, err := cluster.addNode(e2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		for _, ds := range []*Service{ds1, ds2} {
			err = ds.Shutdown(context.Background())
			if err != nil {
				ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
			}
		}
	}()

	// Add listener

	dt1, err := ds1.NewDTopic("my-topic", 0, UnorderedDelivery)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	onMessage := func(msg Message) {
		defer cancel()
		if msg.Message.(string) != "message" {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if msg.PublisherAddr != ds2.rt.This().String() {
			t.Fatalf("Expected %s. Got: %s", ds2.rt.This().String(), msg.PublisherAddr)
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

	dtTwo, err := ds2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e1 := newTestEnvironment(nil)
	ds1, err := cluster.addNode(e1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	e2 := newTestEnvironment(nil)
	ds2, err := cluster.addNode(e2)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		for _, ds := range []*Service{ds1, ds2} {
			err = ds.Shutdown(context.Background())
			if err != nil {
				ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
			}
		}
	}()

	// Add listener

	dt1, err := ds1.NewDTopic("my-topic", 0, UnorderedDelivery)
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

	dt2, err := ds2.NewDTopic("my-topic", 0, UnorderedDelivery)
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
	cluster := newTestCluster()
	e := newTestEnvironment(nil)
	ds, err := cluster.addNode(e)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = ds.Shutdown(context.Background())
		if err != nil {
			ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
		}
	}()
	_, err = ds.NewDTopic("my-topic", 0, 0)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Expected ErrInvalidArgument. Got: %v", err)
	}
}

func TestDTopic_OrderedDelivery(t *testing.T) {
	cluster := newTestCluster()
	e := newTestEnvironment(nil)
	ds, err := cluster.addNode(e)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = ds.Shutdown(context.Background())
		if err != nil {
			ds.log.V(2).Printf("[ERROR] Failed to shutdown the node: %v", err)
		}
	}()
	_, err = ds.NewDTopic("my-topic", 0, OrderedDelivery)
	if err != ErrNotImplemented {
		t.Errorf("Expected ErrNotImplemented. Got: %v", err)
	}
}
