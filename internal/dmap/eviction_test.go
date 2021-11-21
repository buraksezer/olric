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
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_Eviction_TTL(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	s2 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.PutEx(testutil.ToKey(i), testutil.ToVal(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	<-time.After(time.Millisecond)
	for i := 0; i < 100; i++ {
		s1.evictKeys()
		s2.evictKeys()
	}

	length := 0
	for _, ins := range []*Service{s1, s2} {
		for partID := uint64(0); partID < s1.config.PartitionCount; partID++ {
			part := ins.primary.PartitionByID(partID)
			part.Map().Range(func(k, v interface{}) bool {
				f := v.(*fragment)
				length += f.storage.Stats().Length
				return true
			})
		}
	}
	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_Eviction_Config_TTLDuration(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		TTLDuration: 10 * time.Millisecond,
		Engine:      config.NewEngine(),
	}
	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	<-time.After(110 * time.Millisecond)
	for i := 0; i < 100; i++ {
		s.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}
	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_Eviction_Config_MaxIdleDuration(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxIdleDuration: 10 * time.Millisecond,
		Engine:          config.NewEngine(),
	}
	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	<-time.After(15 * time.Millisecond)
	for i := 0; i < 100; i++ {
		s.evictKeys()
	}

	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_Eviction_LRU_Config_MaxKeys(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxKeys:        70,
		EvictionPolicy: config.LRUEviction,
		Engine:         config.NewEngine(),
	}
	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_Eviction_LRU_Config_MaxInuse(t *testing.T) {
	cluster := testcluster.New(NewService)
	c := testutil.NewConfig()
	c.DMaps = &config.DMaps{
		MaxInuse:       2048,
		EvictionPolicy: config.LRUEviction,
		Engine:         testutil.NewEngineConfig(t),
	}

	e := testcluster.NewEnvironment(c)
	s := cluster.AddMember(e).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	length := 0
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := s.primary.PartitionByID(partID)
		part.Map().Range(func(k, v interface{}) bool {
			f := v.(*fragment)
			length += f.storage.Stats().Length
			return true
		})
	}

	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}
