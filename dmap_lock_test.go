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
	"context"
	"testing"
	"time"
)

func TestDMap_Locker_Standalone(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
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
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()
	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err := dm2.Unlock(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_LockWithTwoHosts(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
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
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()
	dm2 := r2.NewDMap("mymap")
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

	err = dm.Unlock("foobar2")
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
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
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
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()
	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.LockWithTimeout(key, 200*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for key: %s", err, key)
		}
	}
	time.Sleep(350 * time.Millisecond)
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.Unlock(key)
		if err != ErrNoSuchLock {
			t.Fatalf("Expected ErrNoSuchLock. Got: %v for key: %s", err, key)
		}
	}
}
