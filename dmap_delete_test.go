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

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
)

func TestDMap_Delete(t *testing.T) {
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
		err = dm.Delete(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		_, err := dm.Get(key)
		if err == nil {
			t.Fatalf("Expected an error. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_DeleteLookup(t *testing.T) {
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
		err = dm3.Delete(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		_, err = dm3.Get(key)
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_DeleteStaleDMaps(t *testing.T) {
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

	db1.deleteStaleDMaps()
	db2.deleteStaleDMaps()

	var dc int32
	for i := 0; i < 1000; i++ {
		dc = 0
		for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
			for _, instance := range []*Olric{db1, db2} {
				part := instance.partitions[partID]
				part.m.Range(func(name, dm interface{}) bool { dc++; return true })

				bpart := instance.backups[partID]
				bpart.m.Range(func(name, dm interface{}) bool { dc++; return true })
			}
		}
		if dc == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dc != 0 {
		t.Fatalf("Expected dmap count is 0. Got: %d", dc)
	}
}

func TestDMap_DeleteOnPreviousOwner(t *testing.T) {
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

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.Put("mykey", []byte("mykey"))
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.NewDMapMessage(protocol.OpDelete)
	req.SetBuffer(new(bytes.Buffer))
	req.SetDMap("mydmap")
	req.SetKey("mykey")
	resp := req.Response(nil)
	db.deletePrevOperation(resp, req)
	if resp.Status() != protocol.StatusOK {
		t.Fatalf("Expected StatusOK (%d). Got: %d", protocol.StatusOK, resp.Status())
	}
	_, err = dm.Get("mykey")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_DeleteKeyValFromPreviousOwners(t *testing.T) {
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

	dm, err := db1.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("mykey", "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Prepare fragmented partition owners list
	hkey := db1.getHKey("mydmap", "mykey")
	owners := db1.getPartitionOwners(hkey)
	owner := owners[len(owners)-1]

	data := []discovery.Member{}
	for _, member := range db1.discovery.GetMembers() {
		if hostCmp(member, owner) {
			continue
		}
		data = append(data, member)
	}
	// this has to be the last one
	data = append(data, owner)
	err = db1.deleteKeyValFromPreviousOwners("mydmap", "mykey", data)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
