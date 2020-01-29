// Copyright 2018-2019 Burak Sezer
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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/hashicorp/memberlist"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func getRandomAddr() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func testConfig(peers []*Olric) *config.Config {
	var speers []string
	if peers != nil {
		for _, peer := range peers {
			speers = append(speers, peer.discovery.LocalNode().Address())
		}
	}
	return &config.Config{
		PartitionCount:    7,
		ReplicaCount:      2,
		WriteQuorum:       1,
		ReadQuorum:        1,
		Peers:             speers,
		KeepAlivePeriod:   10 * time.Millisecond,
		LogVerbosity:      6,
		MemberCountQuorum: config.MinimumMemberCountQuorum,
	}
}

func testSingleReplicaConfig() *config.Config {
	c := testConfig(nil)
	c.ReplicaCount = 1
	c.WriteQuorum = 1
	c.ReadQuorum = 1
	return c
}

func newDB(c *config.Config, peers ...*Olric) (*Olric, error) {
	if c == nil {
		c = testConfig(peers)
	}
	if len(c.Peers) == 0 {
		for _, peer := range peers {
			c.Peers = append(c.Peers, peer.discovery.LocalNode().Address())
		}
	}

	addr, err := getRandomAddr()
	if err != nil {
		return nil, err
	}
	if c.MemberlistConfig == nil {
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	c.MemberlistConfig.Name = addr
	c.MemberlistConfig.BindPort = 0
	c.Name = addr

	db, err := New(c)
	if err != nil {
		return nil, err
	}

	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		err = db.server.ListenAndServe()
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to run TCP server")
		}
	}()
	<-db.server.StartCh

	err = db.startDiscovery()
	if err != nil {
		return nil, err
	}
	// Wait some time for goroutines
	<-time.After(100 * time.Millisecond)
	return db, nil
}

func syncClusterMembers(peers ...*Olric) {
	updateRouting := func () {
		for _, peer := range peers {
			if peer.discovery.IsCoordinator() {
				peer.updateRouting()
			}
		}
	}

	// Update routing table on the cluster before running rebalancer
	updateRouting()
	for _, peer := range peers {
		peer.rebalancer()
	}
	// Update routing table again to get correct responses from the high level DMap API.
	updateRouting()
}

type testCustomConfig struct {
	ReadRepair        bool
	ReplicaCount      int
	WriteQuorum       int
	ReadQuorum        int
	MemberCountQuorum int32
}

func newTestCustomConfig() *testCustomConfig {
	return &testCustomConfig{
		ReadRepair:        false,
		ReplicaCount:      2,
		WriteQuorum:       1,
		ReadQuorum:        1,
		MemberCountQuorum: config.MinimumMemberCountQuorum,
	}
}

type testCluster struct {
	peers  []*Olric
	config *testCustomConfig
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func newTestCluster(c *testCustomConfig) *testCluster {
	ctx, cancel := context.WithCancel(context.Background())
	t := &testCluster{
		config: c,
		peers:  []*Olric{},
		ctx:    ctx,
		cancel: cancel,
	}
	return t
}

func (t *testCluster) teardown() {
	t.cancel()
	t.wg.Wait()
}

func (t *testCluster) newDB() (*Olric, error) {
	c := testConfig(nil)
	if t.config != nil {
		if t.config.ReplicaCount != 0 {
			c.ReplicaCount = t.config.ReplicaCount
		}
		if t.config.WriteQuorum != 0 {
			c.WriteQuorum = t.config.WriteQuorum
		}
		if t.config.ReadQuorum != 0 {
			c.ReadQuorum = t.config.ReadQuorum
		}
		c.ReadRepair = t.config.ReadRepair
		c.MemberCountQuorum = t.config.MemberCountQuorum
	}
	db, err := newDB(c, t.peers...)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers = append(t.peers, db)
	syncClusterMembers(t.peers...)

	t.wg.Add(1)
	go func(db *Olric) {
		<-t.ctx.Done()
		defer t.wg.Done()
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric on %s: %v", db.this, err)
		}
	}(db)
	return db, nil
}

func TestDMap_Standalone(t *testing.T) {
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

	key := "mykey"
	value := "myvalue"
	// Create a new DMap instance and put a K/V pair.
	d, err := db.NewDMap("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = d.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Get the value and check it.
	val, err := d.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != value {
		t.Fatalf("Expected value %v. Got: %v", value, val)
	}

	// Delete it and check again.
	err = d.Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = d.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_PruneHosts(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	eventsDB1 := db1.discovery.SubscribeNodeEvents()
	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-eventsDB1
	syncClusterMembers(db1, db2)

	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	eventsDB2 := db2.discovery.SubscribeNodeEvents()
	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	<-eventsDB1
	<-eventsDB2
	syncClusterMembers(db1, db2, db3)

	instances := []*Olric{db1, db2, db3}
	for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
		for _, db := range instances {
			part := db.partitions[partID]
			if part.ownerCount() != 1 {
				t.Fatalf("Expected owner count is 1. Got: %d", part.ownerCount())
			}
		}
	}
}

func TestDMap_CrashServer(t *testing.T) {
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

	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var maxIteration int
	for {
		<-time.After(10 * time.Millisecond)
		members := db3.discovery.GetMembers()
		if len(members) == 3 {
			break
		}
		maxIteration++
		if maxIteration >= 1000 {
			t.Fatalf("Routing table has not been updated yet: %v", members)
		}
	}

	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	eventsDB2 := db2.discovery.SubscribeNodeEvents()
	eventsDB3 := db3.discovery.SubscribeNodeEvents()
	err = db1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Failed to shutdown Olric: %v", err)
	}
	<-eventsDB2
	<-eventsDB3

	// The new coordinator is db2
	syncClusterMembers(db2, db3)

	dm2, err := db2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		_, err = dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s: %s", err, bkey(i), db1.this)
		}
	}
}
