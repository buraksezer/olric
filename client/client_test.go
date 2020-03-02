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

package client

import (
	"context"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/query"
)

var testConfig = &Config{
	DialTimeout: time.Second,
	KeepAlive:   time.Second,
	MaxConn:     10,
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func newDB() (*olric.Olric, chan struct{}, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	cfg := &config.Config{
		PartitionCount:    7,
		Name:              addr,
		ReplicaCount:      config.MinimumReplicaCount,
		WriteQuorum:       config.MinimumReplicaCount,
		ReadQuorum:        config.MinimumReplicaCount,
		MemberCountQuorum: config.MinimumMemberCountQuorum,
	}
	db, err := olric.New(cfg)
	if err != nil {
		return nil, nil, err
	}

	done := make(chan struct{})
	go func() {
		rerr := db.Start()
		if rerr != nil {
			log.Printf("[ERROR] Expected nil. Got %v", rerr)
		}
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	testConfig.Addrs = []string{addr}
	return db, done, nil
}

func TestClient_Get(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := db.NewDMap(name)
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

func TestClient_Put(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key, value := "my-key", "my-value"
	err = c.NewDMap(name).Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm, err := db.NewDMap(name)
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

func TestClient_PutEx(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	dm, err := db.NewDMap(name)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		serr := db.Shutdown(ctx)
		if serr != nil {
			log.Printf("[WARN] Olric Shutdown returned an error: %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		serr := db.Shutdown(ctx)
		if serr != nil {
			log.Printf("[WARN] Olric Shutdown returned an error: %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		serr := db.Shutdown(ctx)
		if serr != nil {
			log.Printf("[WARN] Olric Shutdown returned an error: %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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

func TestClient_Ping(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	addr := testConfig.Addrs[0]
	err = c.Ping(addr)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_Stats(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	key := "my-key-"
	for i := 0; i < 100; i++ {
		err = c.NewDMap(name).Put(key+strconv.Itoa(i), i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	addr := testConfig.Addrs[0]
	s, err := c.Stats(addr)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var totalByKeyCount int
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
			t.Fatalf("Expected Length is bigger than 0. Got: %d", part.Length)
		}
		totalByKeyCount += part.Length
		if part.Owner.String() != addr {
			t.Fatalf("Expected partition owner: %s. Got: %s", addr, part.Owner)
		}
	}
	if totalByKeyCount != 100 {
		t.Fatalf("Expected total length of partitions in stats is 100. Got: %d", total)
	}
	if total != 100 {
		t.Fatalf("Expected total length of partitions in stats is 100. Got: %d", total)
	}
}

func TestClient_Expire(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	name := "mymap"
	dm, err := db.NewDMap(name)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
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

func TestClient_Query(t *testing.T) {
	db, done, err := newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	defer func() {
		serr := db.Shutdown(context.Background())
		if serr != nil {
			t.Errorf("Expected nil. Got %v", serr)
		}
		<-done
	}()

	c, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm := c.NewDMap("mymap")
	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}
		err = dm.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	q, err := dm.Query(query.M{"$onKey": query.M{"$regexMatch": "even:"}})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = q.Range(func(key string, value interface{}) bool {
		if !strings.HasPrefix(key, "even:") {
			t.Fatalf("Expected prefix: even:. Got: %s", key)
		}
		return true
	})

	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
