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
	"github.com/buraksezer/olric/internal/kvstore"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
)

func TestDMap_Put(t *testing.T) {
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
	dm2, err := db2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), bkey(i))
		}
	}
}

func TestDMap_Put_AsyncReplicationMode(t *testing.T) {
	cfg := newTestCustomConfig()
	cfg.ReplicationMode = config.AsyncReplicationMode
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
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := db2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 10; i++ {
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), bkey(i))
		}
	}
}

func TestDMap_PutEx(t *testing.T) {
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
		err = dm.PutEx(bkey(i), bval(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
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

func TestDMap_PutWriteQuorum(t *testing.T) {
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
	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		key := bkey(i)
		host, _ := db1.findPartitionOwner(dm.name, key)
		if cmpMembersByID(db1.this, host) {
			err = dm.Put(key, bval(i))
			if err != ErrWriteQuorum {
				t.Fatalf("Expected ErrWriteQuorum. Got: %v", err)
			}
			hit = true
		}
	}
	if !hit {
		t.Fatalf("No keys checked on %v", db1)
	}
}

func TestDMap_PutIfNotExist(t *testing.T) {
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

	dm, err := db.NewDMap("mymap")
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
		err = dm.PutIf(bkey(i), bval(i*2), IfNotFound)
		if err == ErrKeyFound {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		val, err := dm.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), bkey(i))
		}
	}
}

func TestDMap_PutIfFound(t *testing.T) {
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

	dm, err := db.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm.PutIf(bkey(i), bval(i*2), IfFound)
		if err == ErrKeyNotFound {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		_, err := dm.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_compactTables(t *testing.T) {
	cfg := testSingleReplicaConfig()
	kv := &kvstore.KVStore{}
	cfg.StorageEngines.Impls[kv.Name()] = kv
	cfg.StorageEngines.Config[kv.Name()] = map[string]interface{}{
		"tableSize": 100,
	}

	db, err := newDB(cfg)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm, err := db.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Compacting tables is an async task. Here we check the number of tables periodically.
	maxCheck := 50
	check := func(i int) bool {
		stats, err := db.Stats()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		for partID, part := range stats.Partitions {
			for name, dmp := range part.DMaps {
				if dmp.NumTables != 1 && i < maxCheck-1 {
					return false
				}
				if dmp.NumTables != 1 && i >= maxCheck-1 {
					t.Fatalf("NumTables=%d on DMap: %s PartID: %d", dmp.NumTables, name, partID)
				}
			}
		}
		return true
	}

	for i := 0; i < maxCheck; i++ {
		if check(i) {
			break
		}
		<-time.After(100 * time.Millisecond)

	}
}
