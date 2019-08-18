// Copyright 2018-2019 Burak Sezer
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
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"strconv"
	"testing"
	"time"
)

func TestPipeline_Put(t *testing.T) {
	db, done, err := newOlric()
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
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Read responses
	for _, res := range responses {
		if res.response.Op != protocol.OpPut {
			t.Fatalf("Expected Op: %v. Got: %v", protocol.OpPut, res.response.Op)
		}
		if res.response.Status != protocol.StatusOK {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusOK, res.response.Status)
		}
	}
}

func TestPipeline_Get(t *testing.T) {
	db, done, err := newOlric()
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
	p := c.NewPipeline()

	dmap := "mydmap"
	key := "key-" + strconv.Itoa(1)

	// Put the key
	err = p.Put(dmap, key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the key
	err = p.Get(dmap, key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Flush commands
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for _, res := range responses {
		if res.Operation() == "Put" {
			err := res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Get" {
			val, err := res.Get()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) != 1 {
				t.Fatalf("Expected 1. Got: %v", val)
			}
		}
	}
}

func TestPipeline_PutEx(t *testing.T) {
	db, done, err := newOlric()
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
	p := c.NewPipeline()

	// Put some keys with ttl
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutEx(dmap, key, i, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Read responses
	for _, res := range responses {
		if res.response.Op != protocol.OpPutEx {
			t.Fatalf("Expected Op: %v. Got: %v", protocol.OpPut, res.response.Op)
		}
		if res.response.Status != protocol.StatusOK {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusOK, res.response.Status)
		}
	}
}

func TestPipeline_Delete(t *testing.T) {
	db, done, err := newOlric()
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
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Delete the keys
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Delete(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Read responses
	for _, res := range responses {
		if res.Operation() == "Put" {
			err := res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Delete" {
			err := res.Delete()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
	}


	// Try to get the keys
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush commands
	responses, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for _, res := range responses {
		_, err := res.Get()
		if err != olric.ErrKeyNotFound {
			t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
		}
	}
}