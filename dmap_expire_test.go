// Copyright 2019 Burak Sezer
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
	"testing"
	"time"
)

func TestDMap_SetExpireStandalone(t *testing.T) {
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
	// Create a new DMap instance and put a K/V pair.
	dm, err := db.NewDMap("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(key, "value")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the value and check it.
	_, err = dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.Expire(key, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_SetExpireKeyNotFound(t *testing.T) {
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
	// Create a new DMap instance and put a K/V pair.
	dm, err := db.NewDMap("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.Expire(key, time.Millisecond)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_ExpireOnCluster(t *testing.T) {
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

	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		err = dm.Expire(bkey(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Wait some time to expire keys properly
	<-time.After(5 * time.Millisecond)

	for i := 0; i < 100; i++ {
		// Try to evict keys here. 100 is a random number
		// to make the test less error prone.
		db1.evictKeys()
		db2.evictKeys()
	}

	for i := 0; i < 10; i++ {
		_, err := dm.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_ExpireWriteQuorum(t *testing.T) {
	cfg := newTestCustomConfig()
	cfg.WriteQuorum = 2
	c := newTestCluster(cfg)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		key := bkey(i)
		err = dm.Put(key, bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = db2.Shutdown(context.Background())
	if err != nil {
		db1.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
	}

	var maxIteration int
	for {
		<-time.After(10 * time.Millisecond)
		members := db1.discovery.GetMembers()
		if len(members) == 1 {
			break
		}
		maxIteration++
		if maxIteration >= 1000 {
			t.Fatalf("Routing table has not been updated yet: %v", members)
		}
	}
	syncClusterMembers(db1)

	var hit bool
	for i := 0; i < 10; i++ {
		key := bkey(i)
		host, _ := db1.findPartitionOwner(dm.name, key)
		if hostCmp(db1.this, host) {
			err = dm.Expire(key, time.Millisecond)
			if err != ErrWriteQuorum {
				t.Fatalf("Expected ErrWriteQuorum. Got: %v", err)
			}
			hit = true
		}
	}

	if !hit {
		t.Fatalf("WriteQuorum check failed %v", db1)
	}
}
