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
	"context"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

var testConfig = &Config{
	Client: &config.Client{
		DialTimeout: time.Second,
		KeepAlive:   time.Second,
		MaxConn:     10,
	},
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
	cfg := &config.Config{
		PartitionCount:    7,
		BindAddr:          "127.0.0.1",
		BindPort:          port,
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
	testConfig.Servers = []string{"127.0.0.1:" + strconv.Itoa(port)}
	return db, done, nil
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

	addr := testConfig.Servers[0]
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

	addr := testConfig.Servers[0]
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
	}
	if totalByKeyCount != 100 {
		t.Fatalf("Expected total length of partitions in stats is 100. Got: %d", total)
	}
	if total != 100 {
		t.Fatalf("Expected total length of partitions in stats is 100. Got: %d", total)
	}
}
