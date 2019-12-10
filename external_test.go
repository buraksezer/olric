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
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
)

func TestExternal_UnknownOperation(t *testing.T) {
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
	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   "mykey",
		Value: []byte("myvalue"),
	}
	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 10,
	}
	c := transport.NewClient(cc)
	resp, err := c.Request(protocol.OpCode(255), m)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status != protocol.StatusErrUnknownOperation {
		t.Fatalf("Expected status code: %d. Got: %d", protocol.StatusErrUnknownOperation, resp.Status)
	}
}

func TestExternal_AtomicIncrDecr(t *testing.T) {
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

	value, err := db.serializer.Marshal(100)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	m := &protocol.Message{
		DMap:  "mydmap",
		Key:   "mykey",
		Value: value,
		Extra: protocol.AtomicExtra{
			Timestamp: time.Now().UnixNano(),
		},
	}
	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 10,
	}
	c := transport.NewClient(cc)
	resp, err := c.Request(protocol.OpIncr, m)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	var val interface{}
	err = db.serializer.Unmarshal(resp.Value, &val)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val.(int) != 100 {
		t.Fatalf("Expected value is 100. Got: %v", val)
	}
}

func TestExternal_AtomicGetPut(t *testing.T) {
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

	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 100,
	}
	c := transport.NewClient(cc)
	var total int64
	var wg sync.WaitGroup
	var final int64
	start := make(chan struct{})

	getput := func(i int) {
		defer wg.Done()
		<-start

		value, err := db.serializer.Marshal(i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		m := &protocol.Message{
			DMap:  "atomic_test",
			Key:   "atomic_getput",
			Value: value,
			Extra: protocol.AtomicExtra{
				Timestamp: time.Now().UnixNano(),
			},
		}
		resp, err := c.Request(protocol.OpGetPut, m)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if len(resp.Value) != 0 {
			var oldval interface{}
			err = db.serializer.Unmarshal(resp.Value, &oldval)
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

	dm, err := db.NewDMap("atomic_test")
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
