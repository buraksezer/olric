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

package dmap

import (
	"github.com/buraksezer/olric/internal/testutil"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
)

// TODO: Add an integration test for write quorum control

func Test_Expire_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	key := "mykey"
	err = dm.Put(key, "myvalue")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	err = dm.Expire(key, time.Millisecond)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func Test_Expire_ErrKeyNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = dm.Expire("mykey", time.Millisecond)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func Test_Expire_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 10; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		err = dm.Expire(testutil.ToKey(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Wait some time to expire keys properly
	<-time.After(5 * time.Millisecond)

	for i := 0; i < 100; i++ {
		// Try to evict keys here. 100 is a random number
		// to make the test less error prone.
		s1.evictKeys()
		s2.evictKeys()
	}

	for i := 0; i < 10; i++ {
		_, err := dm.Get(testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}