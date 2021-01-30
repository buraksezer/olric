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
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
)

func Test_LockWithTimeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func Test_Unlock_After_Timeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := dm.LockWithTimeout(key, time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	<-time.After(2 * time.Millisecond)
	err = ctx.Unlock()
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}
}

func Test_LockWithTimeout_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dm.LockWithTimeout(key, time.Second, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
	if err != ErrLockNotAcquired {
		t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
	}
}

func Test_Lock_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	ctx, err := dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = ctx.Unlock()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func Test_Lock_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = dm.Lock(key, time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	_, err = dm.Lock(key, time.Millisecond)
	if err != ErrLockNotAcquired {
		t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
	}
}

func Test_LockWithTimeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.LockWithTimeout(key, time.Hour, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for _, ctx := range lockContext {
		err = ctx.Unlock()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}

func Test_Lock_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.Lock(key, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for _, ctx := range lockContext {
		err = ctx.Unlock()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}

func Test_LockWithTimeout_ErrLockNotAcquired_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.LockWithTimeout(key, 5*time.Second, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
		if err != ErrLockNotAcquired {
			t.Fatalf("Expected ErrLockNotAcquired. Got: %v", err)
		}
	}
}
