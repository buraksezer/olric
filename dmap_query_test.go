// Copyright 2020 Burak Sezer
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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/query"
	"github.com/vmihailenco/msgpack"
	"strings"
	"testing"
	"time"
)

func TestDMap_QueryOnKeyStandalone(t *testing.T) {
	c := testSingleReplicaConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.Started = func() {
		cancel()
	}

	db, err := newDB(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	<-ctx.Done()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + bkey(i)
		} else {
			key = "odd:" + bkey(i)
		}
		err = dm.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	q, err := dm.Query(
		query.M{
			"$onKey": query.M{
				"$regexMatch": "even:",
			},
		},
	)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	count := 0
	err = q.Range(func(key string, value interface{}) bool {
		count++
		if value.(int)%2 != 0 {
			t.Fatalf("Expected value is an even number. Got: %d", value)
		}
		return true
	})

	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if count != 50 {
		t.Fatalf("Expected count is 50. Got: %d", count)
	}
}

func TestDMap_QueryOnKeyCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm1, err := db1.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + bkey(i)
		} else {
			key = "odd:" + bkey(i)
		}
		err = dm1.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := db2.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	q, err := dm2.Query(
		query.M{
			"$onKey": query.M{
				"$regexMatch": "even:",
			},
		},
	)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer q.Close()

	count := 0
	err = q.Range(func(key string, value interface{}) bool {
		count++
		if value.(int)%2 != 0 {
			t.Fatalf("Expected value is an even number. Got: %d", value)
		}
		return true
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if count != 50 {
		t.Fatalf("Expected count is 50. Got: %d", count)
	}
}

func TestDMap_QueryOnKeyIgnoreValues(t *testing.T) {
	c := testSingleReplicaConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.Started = func() {
		cancel()
	}

	db, err := newDB(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	<-ctx.Done()

	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + bkey(i)
		} else {
			key = "odd:" + bkey(i)
		}
		err = dm.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	q, err := dm.Query(
		query.M{
			"$onKey": query.M{
				"$regexMatch": "even:",
				"$options": query.M{
					"$onValue": query.M{
						"$ignore": true,
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	count := 0
	err = q.Range(func(key string, value interface{}) bool {
		count++
		if value != nil {
			t.Fatalf("Expected nil. Got: %v", value)
		}
		return true
	})

	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if count != 50 {
		t.Fatalf("Expected count is 50. Got: %d", count)
	}
}

func TestDMap_IteratorCluster(t *testing.T) {
	c := newTestCluster(nil)
	defer c.teardown()

	db1, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	db2, err := c.newDB()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm1, err := db1.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + bkey(i)
		} else {
			key = "odd:" + bkey(i)
		}
		err = dm1.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := db2.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	q, err := dm2.Query(
		query.M{
			"$onKey": query.M{
				"$regexMatch": "",
			},
		},
	)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer q.Close()

	count := 0
	err = q.Range(func(key string, value interface{}) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if count != 100 {
		t.Fatalf("Expected count is 100. Got: %d", count)
	}
}

func TestDMap_Query(t *testing.T) {
	c := testSingleReplicaConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.Started = func() {
		cancel()
	}

	db, err := newDB(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	<-ctx.Done()
	dm, err := db.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + bkey(i)
		} else {
			key = "odd:" + bkey(i)
		}
		err = dm.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	q := query.M{
		"$onKey": query.M{
			"$regexMatch": "even:",
		},
	}
	value, err := msgpack.Marshal(q)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	m := &protocol.Message{
		DMap:  "mydmap",
		Value: value,
		Extra: protocol.QueryExtra{PartID: 0},
	}
	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 10,
	}
	cl := transport.NewClient(cc)
	resp, err := cl.Request(protocol.OpQuery, m)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status != protocol.StatusOK {
		t.Fatalf("Expected protocol.StatusOK (%d). Got: %d", protocol.StatusOK, resp.Status)
	}
	var qr QueryResponse
	if err = msgpack.Unmarshal(resp.Value, &qr); err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for key, value := range qr {
		if !strings.HasPrefix(key, "even:") {
			t.Fatalf("Expected prefix is even:. Got: %s", key)
		}

		var v interface{}
		err = db.serializer.Unmarshal(value.([]byte), &v)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if v.(int)%2 != 0 {
			t.Fatalf("Expected even number. Got: %v", v)
		}
	}
}

func TestDMap_QueryEndOfKeySpace(t *testing.T) {
	c := testSingleReplicaConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	c.Started = func() {
		cancel()
	}

	db, err := newDB(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	<-ctx.Done()

	q := query.M{
		"$onKey": query.M{
			"$regexMatch": "even:",
		},
	}
	value, err := msgpack.Marshal(q)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	m := &protocol.Message{
		DMap:  "mydmap",
		Value: value,
		Extra: protocol.QueryExtra{PartID: 300},
	}
	cc := &transport.ClientConfig{
		Addrs:   []string{db.config.Name},
		MaxConn: 10,
	}
	cl := transport.NewClient(cc)
	resp, err := cl.Request(protocol.OpQuery, m)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status != protocol.StatusErrEndOfQuery {
		t.Fatalf("Expected protocol.ErrEndOfQuery (%d). Got: %d", protocol.StatusErrEndOfQuery, resp.Status)
	}
}
