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
)

func TestStatsStandalone(t *testing.T) {
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
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	s, err := db.Stats()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	var total int
	for partID, part := range s.Partitions {
		total += part.Length
		if _, ok := part.DMaps["mymap"]; !ok {
			t.Fatalf("Expected DMap check result is true. Got false")
		}
		if len(part.PreviousOwners) != 0 {
			t.Fatalf("Expected PreviosOwners list is empty. "+
				"Got: %v for PartID: %d", part.PreviousOwners, partID)
		}
		if part.Length <= 0 {
			t.Fatalf("Unexpected Length: %d", part.Length)
		}
		if !hostCmp(part.Owner, db.this) {
			t.Fatalf("Expected partition owner: %s. Got: %s", db.this, part.Owner)
		}
	}
	if total != 100 {
		t.Fatalf("Expected total length of partition in stats is 100. Got: %d", total)
	}
}

func TestStatsCluster(t *testing.T) {
	db1, err := newDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db2, err := newDB(nil, db1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
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
	var primaryTotal int
	var backupTotal int
	for _, db := range []*Olric{db1, db2} {
		s, err := db.Stats()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		for _, part := range s.Partitions {
			primaryTotal += part.Length
		}
		for _, part := range s.Backups {
			backupTotal += part.Length
		}
	}
	if primaryTotal != 100 {
		t.Fatalf("Expected total length of partitions on primary "+
			"owners in stats is 100. Got: %d", primaryTotal)
	}
	if backupTotal != 100 {
		t.Fatalf("Expected total length of partitions on backup "+
			"owners in stats is 100. Got: %d", backupTotal)
	}
}
