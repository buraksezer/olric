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
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
)

func TestDMap_TTLEviction(t *testing.T) {
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
		err = dm.PutEx(bkey(i), bval(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	<-time.After(time.Millisecond)
	for i := 0; i < 100; i++ {
		db1.evictKeys()
		db2.evictKeys()
	}
	length := 0
	for _, ins := range []*Olric{db1, db2} {
		for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
			part := ins.partitions[partID]
			part.m.Range(func(k, v interface{}) bool {
				dm := v.(*dmap)
				length += dm.storage.Len()
				return true
			})
		}
	}
	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_TTLDuration(t *testing.T) {
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

	// This is not recommended but forgivable for testing.
	db.config.Cache = &config.CacheConfig{TTLDuration: 10 * time.Millisecond}

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
	time.Sleep(time.Millisecond)
	for i := 0; i < 100; i++ {
		db.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		part.m.Range(func(k, v interface{}) bool {
			dm := v.(*dmap)
			length += dm.storage.Len()
			return true
		})
	}

	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_TTLMaxIdleDuration(t *testing.T) {
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

	// This is not recommended but forgivable for testing.
	db.config.Cache = &config.CacheConfig{MaxIdleDuration: 10 * time.Millisecond}

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

	<-time.After(11 * time.Millisecond)
	for i := 0; i < 100; i++ {
		db.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		part.m.Range(func(k, v interface{}) bool {
			dm := v.(*dmap)
			length += dm.storage.Len()
			return true
		})
	}

	if length == 10 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_EvictionPolicyLRUMaxKeys(t *testing.T) {
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

	// This is not recommended but forgivable for testing.
	// We have 7 partitions in test setup. So MaxKeys is 10 for every partition.
	db.config.Cache = &config.CacheConfig{MaxKeys: 70, EvictionPolicy: config.LRUEviction}

	dm, err := db.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	keyCount := 0
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		part.m.Range(func(k, v interface{}) bool {
			dm := v.(*dmap)
			keyCount += dm.storage.Len()
			return true
		})
	}
	// We have 7 partitions and a partition may have only 10 keys.
	if keyCount != 70 {
		t.Fatalf("Expected key count is different than 70: %d", keyCount)
	}
}

func TestDMap_EvictionPolicyLRUMaxInuse(t *testing.T) {
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

	// This is not recommended but forgivable for testing.
	// We have 7 partitions in test setup. So MaxInuse is 293 bytes for every partition.
	db.config.Cache = &config.CacheConfig{MaxInuse: 2048, EvictionPolicy: config.LRUEviction}

	dm, err := db.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	keyCount := 0
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		part.m.Range(func(k, v interface{}) bool {
			dm := v.(*dmap)
			keyCount += dm.storage.Len()
			return true
		})
	}
	// Some of the keys should been evicted due to the policy.
	if keyCount == 100 {
		t.Fatalf("Key count has to be smaller than 100: %d", keyCount)
	}
}
