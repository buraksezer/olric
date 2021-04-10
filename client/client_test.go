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
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testolric"
)

func newTestConfig(db *testolric.TestOlric) *Config {
	return &Config{
		Client: &config.Client{
			DialTimeout: time.Second,
			KeepAlive:   time.Second,
			MaxConn:     10,
		},
		Servers: []string{db.Addr},
	}
}

func TestClient_Ping(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	addr := tc.Servers[0]
	err = c.Ping(addr)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func TestClient_Stats(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
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

	addr := tc.Servers[0]
	s, err := c.Stats(addr)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var totalByKeyCount int
	var total int
	for partID, part := range s.Partitions {
		total += part.Length
		if _, ok := part.DMaps["mymap"]; !ok {
			t.Fatalf("Expected dmap check result is true. Got false")
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
