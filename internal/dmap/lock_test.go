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
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
)

func TestDMap_LockWithTimeout_Standalone(t *testing.T) {
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

func TestDMap_Unlock_After_Timeout_Standalone(t *testing.T) {
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

func TestDMap_LockWithTimeout_ErrLockNotAcquired_Standalone(t *testing.T) {
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

func TestDMap_LockLease_Standalone(t *testing.T) {
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

	err = ctx.Lease(2 * time.Second)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	e, err := dm.GetEntry(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if e.TTL-(time.Now().UnixNano()/1000000) <= 1900 {
		t.Fatalf("Expected >=1900. Got: %v", e.TTL-(time.Now().UnixNano()/1000000))
	}

	<-time.After(3 * time.Second)
	err = ctx.Lease(3 * time.Second)
	if err != ErrNoSuchLock {
		t.Fatalf("Expected ErrNoSuchLock. Got: %v", err)
	}
}

func TestDMap_Lock_Standalone(t *testing.T) {
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

func TestDMap_Lock_ErrLockNotAcquired_Standalone(t *testing.T) {
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

func TestDMap_LockWithTimeout_Cluster(t *testing.T) {
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

func TestDMap_LockLease_Cluster(t *testing.T) {
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
		ctx, err := dm.LockWithTimeout(key, 5*time.Second, 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for i := range lockContext {
		err = lockContext[i].Lease(10 * time.Second)
		if err != nil {
			t.Logf("[LOCK]Expected nil. Got: %v", err)
		}
	}
}

func TestDMap_Lock_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
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

func TestDMap_LockWithTimeout_ErrLockNotAcquired_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
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

func TestDMap_Lock_After_LockWithTimeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.LockWithTimeout(key, time.Millisecond, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	cluster.AddMember(nil)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.Lock(key, time.Second)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
}

/*func TestDMap_LockWithTimeout_Operation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var tokens [][]byte
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)

		// Form a LockWithTimeout message manually. Like a client
		r := protocol.NewDMapMessage(protocol.OpLockWithTimeout)
		r.SetDMap("lock.test")
		r.SetBuffer(bytes.NewBuffer(nil))
		r.SetKey(key)
		r.SetExtra(protocol.LockWithTimeoutExtra{
			Timeout:  int64(time.Hour),
			Deadline: int64(time.Second),
		})

		w := r.Response(nil)
		s.lockWithTimeoutOperation(w, r)
		err := w.Decode()
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if w.Status() != protocol.StatusOK {
			t.Fatalf("Expected protoco.StatusOK(%d). Got: %v", protocol.StatusOK, w.Status())
		}
		tokens = append(tokens, w.Value())
	}

	for i, token := range tokens {
		key := "lock.test.foo." + strconv.Itoa(i)
		r := protocol.NewDMapMessage(protocol.OpUnlock)
		r.SetDMap("lock.test")
		r.SetBuffer(bytes.NewBuffer(nil))
		r.SetKey(key)
		r.SetValue(token)

		w := r.Response(nil)
		s.unlockOperation(w, r)
		err := w.Decode()
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if w.Status() != protocol.StatusOK {
			t.Fatalf("Expected protoco.StatusOK(%d). Got: %v", protocol.StatusOK, w.Status())
		}
	}
}*/

/*func TestDMap_Lock_Operation(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	var tokens [][]byte
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)

		// Form a Lock message manually. Like a client
		r := protocol.NewDMapMessage(protocol.OpLock)
		r.SetDMap("lock.test")
		r.SetBuffer(bytes.NewBuffer(nil))
		r.SetKey(key)
		r.SetExtra(protocol.LockExtra{
			Deadline: int64(time.Second),
		})

		w := r.Response(nil)
		s.lockOperation(w, r)
		err := w.Decode()
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if w.Status() != protocol.StatusOK {
			t.Fatalf("Expected protoco.StatusOK(%d). Got: %v", protocol.StatusOK, w.Status())
		}
		tokens = append(tokens, w.Value())
	}

	for i, token := range tokens {
		key := "lock.test.foo." + strconv.Itoa(i)
		r := protocol.NewDMapMessage(protocol.OpUnlock)
		r.SetDMap("lock.test")
		r.SetBuffer(bytes.NewBuffer(nil))
		r.SetKey(key)
		r.SetValue(token)

		w := r.Response(nil)
		s.unlockOperation(w, r)
		err := w.Decode()
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if w.Status() != protocol.StatusOK {
			t.Fatalf("Expected protoco.StatusOK(%d). Got: %v", protocol.StatusOK, w.Status())
		}
	}
}*/

func TestDMap_tryLock(t *testing.T) {
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

	var i int
	var acquired bool
	for i <= 10 {
		i++
		_, err := dm.Lock(key, 100*time.Millisecond)
		if err == ErrLockNotAcquired {
			// already acquired
			continue
		}
		if err != nil {
			// Something went wrong
			t.Fatalf("Expected nil. Got: %v", err)
		}
		// Acquired
		acquired = true
		break
	}
	if !acquired {
		t.Fatal("Failed to acquire lock")
	}
}
