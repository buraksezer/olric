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
	"sync/atomic"
	"testing"
	"time"
)

func TestFSCK_Merge(t *testing.T) {
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

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

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
	for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
		part := r1.partitions[partID]
		part.Lock()
		if len(part.owners) <= 1 {
			part.Unlock()
			continue
		}
		part.Unlock()

		if hostCmp(part.owners[0], r1.this) {
			// Previous owner
			if atomic.LoadInt32(&part.count) != 0 {
				t.Fatalf("Expected map count is 0. Got: %d", atomic.LoadInt32(&part.count))
			}
		}
	}
}

func TestFSCK_MergeWithNewValues(t *testing.T) {
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

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	r1.fsckMx.Lock()

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

	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 101; i++ {
		if i == 3 {
			continue
		}
		err = dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	r1.fsckMx.Unlock()

	var eval []byte
	r1.updateRouting()
	for i := 0; i < 101; i++ {
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if i == 3 {
			eval = bval(i)
		} else {
			eval = []byte(bkey(i) + "-v2")
		}
		if !bytes.Equal(val.([]byte), eval) {
			t.Fatalf("Expected %s. Got: %s", string(eval), string(val.([]byte)))
		}
	}
}

func TestFSCK_MergeWithLock(t *testing.T) {
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

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = dm.LockWithTimeout(bkey(i), time.Minute)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

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

	for partID := uint64(0); partID < r2.config.PartitionCount; partID++ {
		part := r2.partitions[partID]
		if atomic.LoadInt32(&part.count) != 0 {
			t.Fatalf("Expected map count is 0. Got: %d", atomic.LoadInt32(&part.count))
		}
	}

	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm2.Unlock(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	r1.fsck()
	for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
		part := r1.partitions[partID]
		part.Lock()
		if len(part.owners) <= 1 {
			part.Unlock()
			continue
		}
		part.Unlock()
		if hostCmp(part.owners[0], r1.this) {
			// Previous owner
			if atomic.LoadInt32(&part.count) != 0 {
				t.Fatalf("Expected map count is 0. Got: %d", atomic.LoadInt32(&part.count))
			}
		}
	}
}
