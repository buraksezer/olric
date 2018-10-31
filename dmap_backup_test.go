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

package olricdb

import (
	"bytes"
	"context"
	"testing"
)

func TestDMap_PutBackup(t *testing.T) {
	r1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()

	mname := "mymap"
	dm := r1.NewDMap(mname)
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
		var backup = r1
		if hostCmp(owner, r1.this) {
			backup = r2
		}
		partID := r1.getPartitionID(hkey)
		bpart := backup.backups[partID]
		tmp, ok := bpart.m.Load(mname)
		data := tmp.(*dmap)
		if !ok {
			t.Fatalf("mymap could not be found")
		}
		data.Lock()
		vdata, ok := data.d[hkey]
		if !ok {
			t.Fatalf("key: %s could not be found", key)
		}
		var val interface{}
		err = r1.serializer.Unmarshal(vdata.Value, &val)
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
	r1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()
	r1.fsck()

	mname := "mymap"
	dm := r1.NewDMap(mname)
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
		var backup = r1
		if hostCmp(owner, r1.this) {
			backup = r2
		}
		partID := r1.getPartitionID(hkey)
		bpart := backup.backups[partID]
		tmp, ok := bpart.m.Load(mname)
		data := tmp.(*dmap)
		if !ok {
			bpart.Unlock()
			// dmap object is deleted, everything is ok.
			continue
		}
		data.Lock()
		_, ok = data.d[hkey]
		if ok {
			t.Fatalf("key: %s found on backup", key)
		}
		data.Unlock()
	}
}

func TestDMap_GetBackup(t *testing.T) {
	r1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()

	mname := "mymap"
	dm := r1.NewDMap(mname)
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
		var kloc = r1
		if !hostCmp(owner, r1.this) {
			kloc = r2
		}

		m := kloc.getDMap(mname, hkey)
		m.Lock()
		delete(m.d, hkey)
		m.Unlock()

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
	r1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()

	mname := "mymap"
	dm := r1.NewDMap(mname)
	for i := 0; i < 1000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	peers = append(peers, r2.discovery.localNode().Address())
	r3, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r3.Shutdown(context.Background())
		if err != nil {
			r3.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers = append(peers, r3.discovery.localNode().Address())
	r4, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r4.Shutdown(context.Background())
		if err != nil {
			r4.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.deleteStaleDMaps()
	r1.updateRouting()

	for _, bpart := range r1.backups {
		bpart.RLock()
		if len(bpart.owners) != 1 {
			t.Fatalf("Expected backup owner count is 1. Got: %d", len(bpart.owners))
		}
		bpart.RUnlock()
	}
}
