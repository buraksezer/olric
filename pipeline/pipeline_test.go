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

package pipeline

import (
	"context"
	"github.com/buraksezer/olric/internal/protocol"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric"
)

var testConfig = &Config{
	DialTimeout: time.Second,
	KeepAlive:   time.Second,
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

func newOlric() (*olric.Olric, chan struct{}, error) {
	port, err := getFreePort()
	if err != nil {
		return nil, nil, err
	}
	addr := "127.0.0.1:" + strconv.Itoa(port)
	cfg := &olric.Config{Name: addr}
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
	testConfig.Addr = addr
	return db, done, nil
}

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

	p, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	dmap := "mydmap"
	for i := 0; i < 10; i++ {
		key := "key-" + strconv.Itoa(i)
		err = p.Put(dmap, key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	responses, err := p.Flush()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
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

	p, err := New(testConfig)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
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

