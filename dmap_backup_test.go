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
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"testing"
)

func TestDMap_PutBackup(t *testing.T) {
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

	mname := "mymap"
	dm, err := db1.NewDMap(mname)
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
		key := bkey(i)
		hkey := partitions.HKey(mname, key)
		owner := dm.db.primary.PartitionByHKey(hkey).Owner()
		var backup = db1
		if owner.CompareByID(db1.rt.This()) {
			backup = db2
		}
		partID := db1.primary.PartitionIdByHKey(hkey)
		bpart := backup.backup.PartitionById(partID)
		tmp, ok := bpart.Map().Load(mname)
		if !ok {
			t.Fatalf("mymap could not be found")
		}
		data := tmp.(*dmap)
		data.Lock()
		entry, err := data.storage.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var val interface{}
		err = db1.serializer.Unmarshal(entry.Value(), &val)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Fatalf("value is different for key: %s", key)
		}
		data.Unlock()
	}
}

func TestDMap_DeleteBackup(t *testing.T) {
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

	mname := "mymap"
	dm, err := db1.NewDMap(mname)
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
		err = dm.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		key := bkey(i)
		hkey := partitions.HKey(mname, key)
		owner := dm.db.primary.PartitionByHKey(hkey).Owner()
		var backup = db1
		if owner.CompareByID(db1.rt.This()) {
			backup = db2
		}
		partID := db1.backup.PartitionIdByHKey(hkey)
		bpart := backup.backup.PartitionById(partID)
		tmp, ok := bpart.Map().Load(mname)
		data := tmp.(*dmap)
		if !ok {
			bpart.Unlock()
			// dmap instance is deleted, everything is ok.
			continue
		}
		if data.storage.Check(hkey) {
			t.Fatalf("key: %s found on backup", key)
		}
	}
}

func TestDMap_GetBackup(t *testing.T) {
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

	mname := "mymap"
	dm, err := db1.NewDMap(mname)
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
		key := bkey(i)
		hkey := partitions.HKey(mname, key)
		owner := dm.db.primary.PartitionByHKey(hkey).Owner()
		var kloc = db1
		if !owner.CompareByID(db1.rt.This()) {
			kloc = db2
		}

		m, err := kloc.getDMap(mname, hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		m.Lock()
		err = m.storage.Delete(hkey)
		m.Unlock()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		value, err := dm.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("value is different for key: %s", key)
		}
	}
}

// TODO: This test should be revisited after implementing new backup sync algorithm.
/* func TestDMap_PruneStaleBackups(t *testing.T) {
	db1, err := newDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		db1.log.V(2).Printf("db1 closing")
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.LocalNode().Address()}
	db2, err := newDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		db2.log.V(2).Printf("db2 closing")
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 1000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	peers = append(peers, db2.discovery.LocalNode().Address())
	db3, err := newDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		db3.log.V(2).Printf("db3 closing")
		err = db3.Shutdown(context.Background())
		if err != nil {
			db3.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers = append(peers, db3.discovery.LocalNode().Address())
	db4, err := newDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		db4.log.Printf("db4 closing")
		err = db4.Shutdown(context.Background())
		if err != nil {
			db4.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	syncClusterMembers(db1, db2)

	db1.rebalancer()
	for _, bpart := range db1.backup {
		bpart.RLock()
		if len(bpart.owners) != 1 {
			t.Fatalf("Expected backup owner count is 1. Got: %d", len(bpart.owners))
		}
		bpart.RUnlock()
	}
}*/
