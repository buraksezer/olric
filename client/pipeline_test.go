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
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
)

func TestPipeline_Put(t *testing.T) {
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
			t.Fatalf("Expected Op: %v. Got: %v", protocol.OpPutEx, res.response.Op)
		}
		if res.response.Status != protocol.StatusOK {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusOK, res.response.Status)
		}
	}
}

func TestPipeline_Delete(t *testing.T) {
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
			err = res.Put()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
		if res.Operation() == "Delete" {
			err = res.Delete()
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

func TestPipeline_IncrDecr(t *testing.T) {
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
	p := c.NewPipeline()

	// Incr
	dmap := "mydmap"
	key := "mykey"
	for i := 0; i < 10; i++ {
		err = p.Incr(dmap, key, 1)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Decr
	for i := 0; i < 10; i++ {
		err = p.Decr(dmap, key, 1)
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
	for index, res := range responses {
		if res.Operation() == "Incr" {
			val, err := res.Incr()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if index+1 != val {
				t.Fatalf("Expected %v. Got: %v", index+1, val)
			}
		}
		if res.Operation() == "Decr" {
			val, err := res.Decr()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if len(responses)-(index+1) != val {
				t.Fatalf("Expected %v. Got: %v", len(responses)-(index+1), val)
			}
		}
	}
}

func TestPipeline_GetPut(t *testing.T) {
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
	p := c.NewPipeline()

	dmap := "mydmap"
	key := "key-" + strconv.Itoa(1)

	// Put the key
	err = p.Put(dmap, key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Call GetPut to update the key
	err = p.GetPut(dmap, key, 100)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the current value with Get
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
		if res.Operation() == "GetPut" {
			val, err := res.GetPut()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) != 1 {
				t.Fatalf("Expected 1. Got: %v", val)
			}
		}

		if res.Operation() == "Get" {
			val, err := res.Get()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) != 100 {
				t.Fatalf("Expected 100. Got: %v", val)
			}
		}
	}
}

func TestPipeline_Destroy(t *testing.T) {
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
	p := c.NewPipeline()

	dmap := "mydmap"
	key := "key"
	// Put the key
	err = p.Put(dmap, key, 1)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Call GetPut to update the key
	err = p.Destroy(dmap)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Get the current value with Get
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
		if res.Operation() == "Destroy" {
			err := res.Destroy()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}

		if res.Operation() == "Get" {
			_, err := res.Get()
			if err != olric.ErrKeyNotFound {
				t.Fatalf("Expected olric.ErrKeyNotFound. Got: %v", err)
			}
		}
	}
}

func TestPipeline_Expire(t *testing.T) {
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
	p := c.NewPipeline()

	// Put some keys with ttl
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Expire(dmap, key, time.Millisecond)
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
		if res.response.Op != protocol.OpPut && res.response.Op != protocol.OpExpire {
			t.Fatalf("Expected Op: %v or %v. Got: %v",
				protocol.OpPut, protocol.OpExpire, res.response.Op)
		}
		if res.response.Status != protocol.StatusOK {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusOK, res.response.Status)
		}
	}

	<-time.After(200 * time.Millisecond)
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	responses, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Read responses
	for _, res := range responses {
		if res.response.Status != protocol.StatusErrKeyNotFound {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusErrKeyNotFound, res.response.Status)
		}
	}
}

func TestPipeline_PutIf(t *testing.T) {
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
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutIf(dmap, key, (i*100)+1, olric.IfNotFound)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
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
		if res.response.Op == protocol.OpPutIf {
			if res.response.Status != protocol.StatusErrKeyFound {
				t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusErrKeyFound, res.response.Status)
			}
		}

		if res.response.Op == protocol.OpGet {
			val, err := res.Get()
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			if val.(int) > 100 {
				t.Fatalf("Value changed: %v", val)
			}
		}
	}
}

func TestPipeline_PutIfEx(t *testing.T) {
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
	p := c.NewPipeline()

	// Put some keys
	dmap := "mydmap"
	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 1; i <= 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.PutIfEx(dmap, key, (i*100)+1, time.Millisecond, olric.IfFound)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Flush them
	_, err = p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(200 * time.Millisecond)
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Get(dmap, key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// Read responses
	for _, res := range responses {
		if res.response.Status != protocol.StatusErrKeyNotFound {
			t.Fatalf("Expected Status: %v. Got: %v", protocol.StatusErrKeyNotFound, res.response.Status)
		}
	}
}
