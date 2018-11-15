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
	"context"
	"testing"
	"time"
)

func TestDMap_Locker_Standalone(t *testing.T) {
	r, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	key := "mykey"
	value := "myvalue"
	// Create a new DMap object and put a K/V pair.
	d := r.NewDMap("foobar")
	err = d.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = d.LockWithTimeout(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = d.Unlock(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_UnlockWithTwoHosts(t *testing.T) {
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

	dm := db1.NewDMap("mymap")
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
	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err := dm2.Unlock(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_LockWithTwoHosts(t *testing.T) {
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

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

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
	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.LockWithTimeout(key, time.Minute)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for key: %s", err, key)
		}
	}

	err = dm2.Unlock("foobar")
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}

	err = dm.Unlock("foobadb2")
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}

	err = dm.Unlock(bkey(1))
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.LockWithTimeout(bkey(1), time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_Locker_LockWithTimeout(t *testing.T) {
	r, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	key := "mykey"
	value := "myvalue"
	// Create a new DMap object and put a K/V pair.
	d := r.NewDMap("foobar")
	err = d.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = d.LockWithTimeout(key, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	err = d.Unlock(key)
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}

	err = d.LockWithTimeout(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = d.Unlock(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_LockWithTimeoutOnNetwork(t *testing.T) {
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

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

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

	//db1.updateRouting()

	// Block fsck and partition manager. The code should inspect the key/lock
	// on a previous partition owner.
	db1.fsckMx.Lock()
	defer db1.fsckMx.Unlock()

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.LockWithTimeout(key, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for key: %s", err, key)
		}
	}
	// TODO: This is too error prone. Check the lock in a loop.
	time.Sleep(350 * time.Millisecond)
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.Unlock(key)
		if err != ErrNoSuchLock {
			t.Fatalf("Expected ErrNoSuchLock. Got: %v for key: %s", err, key)
		}
	}
}

func TestDMap_LockPrevious(t *testing.T) {
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

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	db1.fsckMx.Lock()
	defer db1.fsckMx.Unlock()

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

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.LockWithTimeout(key, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for key: %s", err, key)
		}
	}
	// TODO: This is too error prone. Check the lock in a loop.
	time.Sleep(350 * time.Millisecond)
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.Unlock(key)
		if err != ErrNoSuchLock {
			t.Fatalf("Expected ErrNoSuchLock. Got: %v for key: %s", err, key)
		}
	}
}
