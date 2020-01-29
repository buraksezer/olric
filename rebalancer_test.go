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
	"bytes"
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestRebalance_Merge(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 1000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
		part := db1.partitions[partID]
		if !hostCmp(part.owner(), db1.this) {
			if part.length() != 0 {
				t.Fatalf("Expected key count is 0 for PartID: %d on %s. Got: %d",
					partID, db1.this, part.length())
			}
		}
	}

	for partID := uint64(0); partID < db2.config.PartitionCount; partID++ {
		part := db2.partitions[partID]
		if hostCmp(part.owner(), db2.this) {
			if part.length() == 0 {
				t.Fatalf("Expected key count is different than zero for PartID: %d on %s", partID, db2.this)
			}
		}
	}
}

func TestRebalance_MergeWithNewValues(t *testing.T) {
	db1, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

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
		hkey := db1.getHKey("mymap", bkey(i))
		underlying, err := db1.getDMap("mymap", hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		vdata, err := underlying.storage.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		vdata.Timestamp = time.Now().Add(60 * time.Minute).UnixNano()
		err = underlying.storage.Put(hkey, vdata)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	db2, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm2, err := db2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peer := net.JoinHostPort(
		db1.config.MemberlistConfig.BindAddr,
		strconv.Itoa(db1.config.MemberlistConfig.BindPort))
	_, err = db2.discovery.Rejoin([]string{peer})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	syncClusterMembers(db1, db2)

	for i := 0; i < 100; i++ {
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Fatalf("Expected %s. Got: %s", string(bval(i)), string(val.([]byte)))
		}
	}
}

func TestRebalance_MergeBackups(t *testing.T) {
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
	for i := 0; i < 1000; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	checkOwnerCount := func(db *Olric) {
		syncClusterMembers(db1, db2, db3)
		for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
			backup := db.backups[partID]
			if backup.ownerCount() != 1 {
				t.Fatalf("Expected backup owner count is 1 for PartID: %d on %s. Got: %d",
					partID, db.this, backup.ownerCount())
			}

			part := db.partitions[partID]
			for _, backupOwner := range backup.loadOwners() {
				if hostCmp(backupOwner, part.owner()) {
					t.Fatalf("Partition owner is also backup owner. PartID: %d: %s",
						partID, backupOwner)
				}
			}
		}
	}

	checkOwnerCount(db1)
	checkOwnerCount(db2)
	checkOwnerCount(db3)
}

func TestRebalance_CheckOwnership(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	checkOwnership := func(db *Olric) {
		for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
			backup := db.backups[partID]
			part := db.partitions[partID]
			members := db.discovery.GetMembers()
			if len(members) == 1 && len(backup.loadOwners()) != 0 {
				t.Fatalf("Invalid ownership distribution")
			}
			for _, backupOwner := range backup.loadOwners() {
				if hostCmp(backupOwner, part.owner()) {
					t.Fatalf("Partition owner is also backup owner. PartID: %d: %s",
						partID, backupOwner)
				}
			}
		}
	}

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	checkOwnership(db1)

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	checkOwnership(db2)

	db3, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	checkOwnership(db3)
}

func TestSplitBrain_ErrClusterQuorum(t *testing.T) {
	cfg := newTestCustomConfig()
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

	db1.config.MemberCountQuorum = 3
	db2.config.MemberCountQuorum = 3

	mname := "mymap"
	_, err = db1.NewDMap(mname)
	if err != ErrClusterQuorum {
		t.Fatalf("Expected ErrClusterQuorum. Got: %v", err)
	}

	_, err = db2.NewDMap(mname)
	if err != ErrClusterQuorum {
		t.Fatalf("Expected ErrClusterQuorum. Got: %v", err)
	}
}

func TestSplitBrain_SimpleMerge(t *testing.T) {
	cfg1 := newTestCustomConfig()
	cfg1.ReplicaCount = 1
	c1 := newTestCluster(cfg1)
	defer c1.teardown()

	db1, err := c1.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db1.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// create a new cluster which is totally unaware about the previous one.
	cfg2 := newTestCustomConfig()
	cfg2.ReplicaCount = 1
	c2 := newTestCluster(cfg2)
	defer c2.teardown()

	db2, err := c2.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm2, err := db2.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm2.Put(bkey(i), bval(i*2))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Merge the clusters.
	port := strconv.Itoa(db2.config.MemberlistConfig.BindPort)
	peer := net.JoinHostPort(db2.config.MemberlistConfig.BindAddr, port)
	_, err = db1.discovery.Rejoin([]string{peer})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Wait for a while for background tasks
	<-time.After(time.Second)

	// After merging we expect accessing the up-to-date versions of the keys.
	for i := 0; i < 100; i++ {
		val, err := dm.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i*2)) {
			t.Fatalf("Expected value: %v. Got: %v", string(bval(i*2)), string(val.([]byte)))
		}
	}
}
