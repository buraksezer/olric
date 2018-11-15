// Copyright 2018 Burak Sezer
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
)

func TestDMap_PutBackup(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		owner, hkey, err := dm.db.locateKey(mname, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var backup = db1
		if hostCmp(owner, db1.this) {
			backup = db2
		}
		partID := db1.getPartitionID(hkey)
		bpart := backup.backups[partID]
		tmp, ok := bpart.m.Load(mname)
		data := tmp.(*dmap)
		if !ok {
			t.Fatalf("mymap could not be found")
		}
		data.Lock()
		vdata, err := data.off.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var val interface{}
		err = db1.serializer.Unmarshal(vdata.Value, &val)
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
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()
	db1.fsck()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		err = dm.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		owner, hkey, err := dm.db.locateKey(mname, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var backup = db1
		if hostCmp(owner, db1.this) {
			backup = db2
		}
		partID := db1.getPartitionID(hkey)
		bpart := backup.backups[partID]
		tmp, ok := bpart.m.Load(mname)
		data := tmp.(*dmap)
		if !ok {
			bpart.Unlock()
			// dmap object is deleted, everything is ok.
			continue
		}
		if data.off.Check(hkey) {
			t.Fatalf("key: %s found on backup", key)
		}
	}
}

func TestDMap_GetBackup(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		owner, hkey, err := dm.db.locateKey(mname, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var kloc = db1
		if !hostCmp(owner, db1.this) {
			kloc = db2
		}

		m, err := kloc.getDMap(mname, hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = m.off.Delete(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		value, err := dm.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Value is different for key: %s", key)
		}
	}
}

func TestDMap_PruneStaleBackups(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 1000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	peers = append(peers, db2.discovery.localNode().Address())
	db3, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db3.Shutdown(context.Background())
		if err != nil {
			db3.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers = append(peers, db3.discovery.localNode().Address())
	r4, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r4.Shutdown(context.Background())
		if err != nil {
			r4.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.deleteStaleDMaps()
	db1.updateRouting()

	for _, bpart := range db1.backups {
		bpart.RLock()
		if len(bpart.owners) != 1 {
			t.Fatalf("Expected backup owner count is 1. Got: %d", len(bpart.owners))
		}
		bpart.RUnlock()
	}
}
