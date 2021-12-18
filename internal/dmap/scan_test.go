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
	"context"
	"testing"

	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func testScanIterator(t *testing.T, s *Service, allKeys map[string]bool) {
	ctx := context.TODO()
	rc := s.respClient.Get(s.rt.This().String())

	var totalKeys int
	var partID, cursor uint64
	for {
		r := resp.NewScan(partID, "mydmap", cursor)
		cmd := r.Command(ctx)
		err := rc.Process(ctx, cmd)
		require.NoError(t, err)

		var keys []string
		keys, cursor, err = cmd.Result()
		require.NoError(t, err)
		totalKeys += len(keys)

		for _, key := range keys {
			_, ok := allKeys[key]
			require.True(t, ok)
			allKeys[key] = true
		}
		if cursor == 0 {
			if partID+1 < s.config.PartitionCount {
				partID++
				continue
			}
			break
		}
	}
	require.Equal(t, 100, totalKeys)

	for _, value := range allKeys {
		require.True(t, value)
	}
}

func TestDMap_scanCommandHandler_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), i)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}

	testScanIterator(t, s, allKeys)
}

func TestDMap_scanCommandHandler_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), i)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}
	testScanIterator(t, s2, allKeys)
}

/*
func TestDMap_QueryOnKeyStandalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + testutil.ToKey(i)
		} else {
			key = "odd:" + testutil.ToKey(i)
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
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + testutil.ToKey(i)
		} else {
			key = "odd:" + testutil.ToKey(i)
		}
		err = dm1.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
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
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + testutil.ToKey(i)
		} else {
			key = "odd:" + testutil.ToKey(i)
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
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + testutil.ToKey(i)
		} else {
			key = "odd:" + testutil.ToKey(i)
		}
		err = dm1.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	dm2, err := s2.NewDMap("mymap")
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
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + testutil.ToKey(i)
		} else {
			key = "odd:" + testutil.ToKey(i)
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

	req := protocol.NewDMapMessage(protocol.OpQuery)
	req.SetDMap("mydmap")
	req.SetValue(value)
	req.SetExtra(protocol.QueryExtra{PartID: 0})

	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	cl := transport.NewClient(cc)
	resp, err := cl.RequestTo(s.rt.This().String(), req)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status() != protocol.StatusOK {
		t.Fatalf("Expected protocol.StatusOK (%d). Got: %d", protocol.StatusOK, resp.Status())
	}
	var qr QueryResponse
	if err = msgpack.Unmarshal(resp.Value(), &qr); err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for key, value := range qr {
		if !strings.HasPrefix(key, "even:") {
			t.Fatalf("Expected prefix is even:. Got: %s", key)
		}

		var v interface{}
		err = s.serializer.Unmarshal(value.([]byte), &v)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if v.(int)%2 != 0 {
			t.Fatalf("Expected even number. Got: %v", v)
		}
	}
}

func TestDMap_QueryEndOfKeySpace(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	q := query.M{
		"$onKey": query.M{
			"$regexMatch": "even:",
		},
	}
	value, err := msgpack.Marshal(q)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	req := protocol.NewDMapMessage(protocol.OpQuery)
	req.SetDMap("mydmap")
	req.SetValue(value)
	req.SetExtra(protocol.QueryExtra{PartID: 300})

	cc := &config.Client{
		MaxConn: 10,
	}
	cc.Sanitize()
	cl := transport.NewClient(cc)
	resp, err := cl.RequestTo(s.rt.This().String(), req)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if resp.Status() != protocol.StatusErrEndOfQuery {
		t.Fatalf("Expected protocol.ErrEndOfQuery (%d). Got: %d", protocol.StatusErrEndOfQuery, resp.Status())
	}
}*/
