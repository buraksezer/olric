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
	"strconv"
	"testing"
	"time"
)

func TestDMap_LockWithTimeoutStandalone(t *testing.T) {
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

	key := "lock.test.foo"
	d, err := db.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := d.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_UnlockWithTimeoutStandalone(t *testing.T) {
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

	key := "lock.test.foo"
	d, err := db.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := d.LockWithTimeout(key, time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(2 * time.Millisecond)
	err = ctx.Unlock()
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}
}

func TestDMap_LockWithTimeoutWaitStandalone(t *testing.T) {
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

	key := "lock.test.foo"
	d, err := db.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = d.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = d.LockWithTimeout(key, time.Second, time.Millisecond)
	if err != ErrLockNotAcquired {
		t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
	}
}

func TestDMap_LockStandalone(t *testing.T) {
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

	key := "lock.test.foo"
	d, err := db.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := d.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestDMap_LockWaitStandalone(t *testing.T) {
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

	key := "lock.test.foo"
	d, err := db.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = d.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = d.Lock(key, time.Millisecond)
	if err != ErrLockNotAcquired {
		t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
	}
}

func TestDMap_LockWithTimeoutCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	d, err := db1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	lockContext := []*LockContext{}
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := d.LockWithTimeout(key, time.Hour, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for _, ctx := range lockContext {
		err = ctx.Unlock()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}

func TestDMap_LockCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	d, err := db1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	lockContext := []*LockContext{}
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := d.Lock(key, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for _, ctx := range lockContext {
		err = ctx.Unlock()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}

func TestDMap_LockWithTimeoutWaitCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	d, err := db1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = d.LockWithTimeout(key, time.Second, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = d.LockWithTimeout(key, time.Second, time.Millisecond)
		if err != ErrLockNotAcquired {
			t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
		}
	}
}

func TestDMap_LockWithTimeoutLockAgainCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	d, err := db1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = d.LockWithTimeout(key, time.Millisecond, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	_, err = c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = d.Lock(key, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}
