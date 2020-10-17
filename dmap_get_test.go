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
)

func TestDMap_Get(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
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

	for i := 0; i < 100; i++ {
		key := bkey(i)
		value, err := dm.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value retrieved for %s", bkey(i))
		}
	}
}

func TestDMap_GetLookup(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
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

	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm3, err := db3.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		key := bkey(i)
		value, err := dm3.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value retrieved for %s", bkey(i))
		}
	}
}

func TestDMap_NilValue(t *testing.T) {
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
	// Create a new dmap instance and put a K/V pair.
	dm, err := db.NewDMap("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put(key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Get the value and check it.
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected value nil. Got: %v", val)
	}

	// Delete it and check again.
	err = dm.Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dm.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_NilValueWithTwoMembers(t *testing.T) {
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
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := db2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		// Get the value and check it.
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if val != nil {
			t.Fatalf("Expected value nil. Got: %v", val)
		}
	}
}

func TestDMap_GetReadQuorum(t *testing.T) {
	cfg := newTestCustomConfig()
	cfg.ReadQuorum = 2
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
		if cmpMembersByID(db1.this, host) {
			_, err = dm.Get(key)
			if err != ErrReadQuorum {
				t.Errorf("Expected ErrReadQuorum. Got: %v", err)
			}
			hit = true
		}
	}
	if !hit {
		t.Fatalf("No keys checked on %v", db1)
	}
}

func TestDMap_ReadRepair(t *testing.T) {
	cfg := newTestCustomConfig()
	cfg.ReadRepair = true
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
		if i%2 == 0 {
			err = dm.PutEx(bkey(i), bval(i), time.Minute)
		} else {
			err = dm.Put(bkey(i), bval(i))
		}
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

	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 10; i++ {
		val, err := dm.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Fatalf("Expected the same value. Got: %s", string(val.([]byte)))
		}
	}
	for i := 0; i < 10; i++ {
		hkey := db3.getHKey("mymap", bkey(i))
		dm3, err := db3.getBackupDMap("mymap", hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		dm3.RLock()
		owners := db3.getBackupPartitionOwners(hkey)
		if cmpMembersByID(owners[0], db3.this) {
			vdata, err := dm3.storage.Get(hkey)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if vdata.Key != bkey(i) {
				t.Fatalf("Expected %s. Got: %s", vdata.Key, bkey(i))
			}
			if bytes.Equal(vdata.Value, bval(i)) {
				t.Fatalf("Expected %s. Got: %s", string(vdata.Value), string(bval(i)))
			}
		}
		dm3.RUnlock()
	}
}
