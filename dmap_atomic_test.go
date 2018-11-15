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
	"sync"
	"sync/atomic"
	"testing"
)

func TestDMap_Incr(t *testing.T) {
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

	var wg sync.WaitGroup
	var start chan struct{}
	key := "incr"

	incr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Incr(key, 1)
		if err != nil {
			r.log.Printf("[ERROR] Failed to call Incr: %v", err)
			return
		}
	}

	dm := r.NewDMap("atomic_test")
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

func TestDMap_Decr(t *testing.T) {
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

	var wg sync.WaitGroup
	var start chan struct{}
	key := "decr"

	decr := func(dm *DMap) {
		<-start
		defer wg.Done()

		_, err := dm.Decr(key, 1)
		if err != nil {
			r.log.Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
	}

	dm := r.NewDMap("atomic_test")
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

func TestDMap_GetPut(t *testing.T) {
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

	var total int64
	var wg sync.WaitGroup
	var start chan struct{}
	key := "getput"
	getput := func(dm *DMap, i int) {
		<-start
		defer wg.Done()

		oldval, err := dm.GetPut(key, i)
		if err != nil {
			r.log.Printf("[ERROR] Failed to call Decr: %v", err)
			return
		}
		if oldval != nil {
			atomic.AddInt64(&total, int64(oldval.(int)))
		}
	}

	dm := r.NewDMap("atomic_test")
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
