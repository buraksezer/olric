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
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/cluster/routingtable"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_Get_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Call DMap.Get on S2
	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := dm2.Get(testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), testutil.ToVal(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), testutil.ToKey(i))
		}
	}
}

func TestDMap_Get_Lookup(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	s3 := cluster.AddMember(nil).(*Service)
	// Call DMap.Get on S3
	dm3, err := s3.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := dm3.Get(testutil.ToKey(i))
		if err != nil {
			fmt.Println(testutil.ToKey(i))
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), testutil.ToVal(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), testutil.ToKey(i))
		}
	}
}

func TestDMap_Get_NilValue(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("foobar", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	val, err := dm.Get("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected value: nil. Got: %v", val)
	}

	err = dm.Delete("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Get("foobar")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_Get_NilValue_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Put("foobar", nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm2, err := s2.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	val, err := dm2.Get("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected value: nil. Got: %v", val)
	}

	err = dm2.Delete("foobar")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm2.Get("foobar")
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_Put_ReadQuorum(t *testing.T) {
	cluster := testcluster.New(NewService)
	// Create DMap services with custom configuration
	c := testutil.NewConfig()
	c.ReplicaCount = 2
	c.ReadQuorum = 2
	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dm.Get(testutil.ToKey(1))
	if err != ErrReadQuorum {
		t.Fatalf("Expected ErrReadQuorum. Got: %v", err)
	}
}

func TestDMap_Get_ReadRepair(t *testing.T) {
	cluster := testcluster.New(NewService)
	c1 := testutil.NewConfig()
	c1.ReadRepair = true
	c1.ReplicaCount = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReadRepair = true
	c2.ReplicaCount = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm1, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm1.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = s2.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	rt := e2.Get("routingtable").(*routingtable.RoutingTable)
	err = rt.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	c3 := testutil.NewConfig()
	c3.ReadRepair = true
	c3.ReplicaCount = 2
	e3 := testcluster.NewEnvironment(c3)
	s3 := cluster.AddMember(e3).(*Service)

	// Call DMap.Get on S2
	dm2, err := s3.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		val, err := dm2.Get(testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), testutil.ToVal(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), testutil.ToKey(i))
		}
	}
}

func TestDMap_GetEntry_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil) // second member
	defer cluster.Shutdown()

	// Call DMap.Put on S1
	dm, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm.PutEx(testutil.ToKey(i), testutil.ToVal(i), time.Hour)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Call DMap.GetEntry
	for i := 0; i < 10; i++ {
		key := testutil.ToKey(i)
		entry, err := dm.GetEntry(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		if !bytes.Equal(entry.Value.([]byte), testutil.ToVal(i)) {
			t.Fatalf("Different value retrieved for %s", testutil.ToKey(i))
		}
		if entry.TTL == 0 {
			t.Fatalf("TTL is empty")
		}
		if entry.Timestamp == 0 {
			t.Fatalf("Timestamp is zero")
		}
	}
}
