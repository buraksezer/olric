// Copyright 2018-2021 Burak Sezer
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

package dmap

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/transport"
)

func TestDMap_Atomic_Incr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incr"

	incr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Incr(key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Incr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go incr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != 100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
}

func TestDMap_Atomic_Decr(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var wg sync.WaitGroup
	var start chan struct{}
	key := "decr"

	decr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Decr(key, 1)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
	}

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	start = make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go decr(dm)
	}
	close(start)
	wg.Wait()

	res, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if res.(int) != -100 {
		t.Fatalf("Expected 100. Got: %v", res)
	}
}

func TestDMap_Atomic_Honor_Current_TTL(t *testing.T) {
	// See https://github.com/buraksezer/olric/pull/172
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "decr"

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.PutEx(key, 10, time.Hour)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Decr(key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	entry, err := dm.GetEntry(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if entry.TTL == 0 {
		t.Fatal("entry.TTL cannot be zero")
	}
}

func TestDMap_Atomic_GetPut(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var total int64
	var wg sync.WaitGroup
	var start chan struct{}
	key := "getput"
	getput := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		oldval, err := dm.GetPut(key, i)
		if err != nil {
			s.log.V(2).Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
		if oldval != nil {
			atomic.AddInt64(&total, int64(oldval.(int)))
		}
	}

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	start = make(chan struct{})
	var final int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(dm, i)
		final += int64(i)
	}
	close(start)
	wg.Wait()

	last, err := dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	atomic.AddInt64(&total, int64(last.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}

func TestDMap_exIncrDecrOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	value, err := s.serializer.Marshal(100)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.NewDMapMessage(protocol.OpIncr)
	req.SetDMap("mydmap")
	req.SetKey("mykey")
	req.SetValue(value)
	req.SetExtra(protocol.AtomicExtra{
		Timestamp: time.Now().UnixNano(),
	})
	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	c := transport.NewClient(cc)
	resp, err := c.RequestTo(s.rt.This().String(), req)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	var val interface{}
	err = s.serializer.Unmarshal(resp.Value(), &val)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(int) != 100 {
		t.Fatalf("Expected value is 100. Got: %v", val)
	}
}

func TestDMap_exGetPutOperation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cc := &config.Client{
		MaxConn: 100,
	}
	cc.Sanitize()
	c := transport.NewClient(cc)
	var total int64
	var wg sync.WaitGroup
	var final int64
	start := make(chan struct{})

	getput := func(i int) {
		defer wg.Done()
		<-start

		value, err := s.serializer.Marshal(i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		req := protocol.NewDMapMessage(protocol.OpGetPut)
		req.SetDMap("atomic_test")
		req.SetKey("atomic_getput")
		req.SetValue(value)
		req.SetExtra(protocol.AtomicExtra{
			Timestamp: time.Now().UnixNano(),
		})

		resp, err := c.RequestTo(s.rt.This().String(), req)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if len(resp.Value()) != 0 {
			var oldval interface{}
			err = s.serializer.Unmarshal(resp.Value(), &oldval)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}

			if oldval != nil {
				atomic.AddInt64(&total, int64(oldval.(int)))
			}
		}
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go getput(i)
		final += int64(i)
	}

	close(start)
	wg.Wait()

	dm, err := s.NewDMap("atomic_test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	result, err := dm.Get("atomic_getput")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	atomic.AddInt64(&total, int64(result.(int)))
	if atomic.LoadInt64(&total) != final {
		t.Fatalf("Expected %d. Got: %d", final, atomic.LoadInt64(&total))
	}
}
