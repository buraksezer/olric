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

package client

import (
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/testolric"
)

func TestClient_Get(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	key, value := "my-key", "my-value"
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := c.NewDMap(name).Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_GetEntry(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	key, value := "my-key", "my-value"
	err = dm.PutEx(key, value, time.Hour)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	entry, err := c.NewDMap(name).GetEntry(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if entry.Value.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", entry.Value.(string), value)
	}
	if entry.Timestamp == 0 {
		t.Fatalf("Timestamp is invalid")
	}
	if entry.TTL == 0 {
		t.Fatalf("TTL is invalid")
	}
}

func TestClient_Put(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}

	t.Run("check olric.Entry fields", func(t *testing.T) {
		entry, err := dm.GetEntry(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if entry.Value.(string) != value {
			t.Fatalf("Expected value %s. Got: %s", entry.Value.(string), value)
		}
		if entry.Timestamp == 0 {
			t.Fatalf("Timestamp is invalid")
		}
		if entry.TTL != 0 {
			t.Fatalf("TTL is invalid")
		}
	})
}

func TestClient_PutEx(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).PutEx(key, value, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(time.Millisecond)
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}

func TestClient_Delete(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.NewDMap(name).Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestClient_LockWithTimeout(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_LockWithTimeoutAwaitOtherLock(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	_, err = dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
	if err != olric.ErrLockNotAcquired {
		t.Fatalf("Expected olric.ErrLockNotAcquired. Got: %v", err)
	}
}

func TestClient_Lock(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	ctx, err := dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_LockAwaitOtherLock(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "lock.test"
	key := "lock.test.key"
	dm := c.NewDMap(name)
	_, err = dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Lock(key, time.Millisecond)
	if err != olric.ErrLockNotAcquired {
		t.Fatalf("Expected olric.ErrLockNotAcquired . Got: %v", err)
	}
}

func TestClient_Destroy(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key := "my-key"
	err = c.NewDMap(name).Put(key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = c.NewDMap(name).Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestClient_Incr(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	key := "incr"
	name := "atomic_test"
	var wg sync.WaitGroup
	start := make(chan struct{})

	incr := func() {
		defer wg.Done()
		<-start

		_, ierr := c.NewDMap(name).Incr(key, 1)
		if ierr != nil {
			log.Printf("[ERROR] Failed to call Incr: %v", ierr)
			return
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr()
	}

	close(start)
	wg.Wait()

	res, err := c.NewDMap(name).Incr(key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if res != 101 {
		t.Fatalf("Expected 101. Got: %v", res)
	}
}

func TestClient_Decr(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	key := "incr"
	name := "atomic_test"
	var wg sync.WaitGroup
	start := make(chan struct{})

	incr := func() {
		defer wg.Done()
		<-start

		_, ierr := c.NewDMap(name).Decr(key, 1)
		if ierr != nil {
			log.Printf("[ERROR] Failed to call Decr: %v", ierr)
			return
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr()
	}

	close(start)
	wg.Wait()

	res, err := c.NewDMap(name).Decr(key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if res != -101 {
		t.Fatalf("Expected -101. Got: %v", res)
	}
}

func TestClient_GetPut(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	key := "getput"
	name := "atomic_test"
	var total int64
	var wg sync.WaitGroup
	var final int64
	start := make(chan struct{})

	getput := func(i int) {
		defer wg.Done()
		<-start

		oldval, ierr := c.NewDMap(name).GetPut(key, i)
		if ierr != nil {
			log.Printf("[ERROR] Failed to call GetPut: %v", ierr)
			return
		}
		if oldval != nil {
			atomic.AddInt64(&total, int64(oldval.(int)))
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(i)
		final += int64(i)
	}

	close(start)
	wg.Wait()

	last, err := c.NewDMap(name).Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	atomic.AddInt64(&total, int64(last.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}

func TestClient_Expire(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := srv.Olric().NewDMap(name)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	key, value := "my-key", "my-value"
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = c.NewDMap(name).Expire(key, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	time.Sleep(time.Millisecond)

	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}

func TestClient_PutIf(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	dm := c.NewDMap(name)
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.PutIf(key, "new-value", olric.IfNotFound)
	if err == olric.ErrKeyFound {
		err = nil
	}
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	val, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(string) != value {
		t.Fatalf("Expected value %s. Got: %s", val.(string), value)
	}
}

func TestClient_PutIfEx(t *testing.T) {
	srv, err := testolric.NewOlric(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	dm := c.NewDMap(name)
	err = dm.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.PutIfEx(key, value, time.Millisecond, olric.IfFound)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	<-time.After(100 * time.Millisecond)
	v, err := dm.Get(key)
	if err != olric.ErrKeyNotFound {
		t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
	}
	if v != nil {
		t.Fatalf("Expected nil. Got: %v", v)
	}
}
