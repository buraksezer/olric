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
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
)

func TestDMap_LockWithTimeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	require.NoError(t, err)

	err = ctx.Unlock()
	require.NoError(t, err)
}

func TestDMap_Unlock_After_Timeout_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout(key, time.Millisecond, time.Second)
	require.NoError(t, err)

	<-time.After(10 * time.Millisecond)

	err = ctx.Unlock()
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_LockWithTimeout_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	_, err = dm.LockWithTimeout(key, time.Second, time.Second)
	require.NoError(t, err)

	_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestDMap_LockLease_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	require.NoError(t, err)

	err = ctx.Lease(2 * time.Second)
	require.NoError(t, err)

	e, err := dm.get(key)
	require.NoError(t, err)

	if e.TTL()-(time.Now().UnixNano()/1000000) <= 1900 {
		t.Fatalf("Expected >=1900. Got: %v", e.TTL()-(time.Now().UnixNano()/1000000))
	}

	<-time.After(3 * time.Second)

	err = ctx.Lease(3 * time.Second)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_Lock_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.Lock(key, time.Second)
	require.NoError(t, err)

	err = ctx.Unlock()
	require.NoError(t, err)
}

func TestDMap_Lock_ErrLockNotAcquired_Standalone(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	_, err = dm.Lock(key, time.Second)
	require.NoError(t, err)

	_, err = dm.Lock(key, time.Millisecond)
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestDMap_LockWithTimeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	require.NoError(t, err)

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.LockWithTimeout(key, time.Hour, time.Second)
		require.NoError(t, err)
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for _, ctx := range lockContext {
		err = ctx.Unlock()
		require.NoError(t, err)
	}
}

func TestDMap_LockLease_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("lock.test")
	require.NoError(t, err)

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.LockWithTimeout(key, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, err)
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for i := range lockContext {
		err = lockContext[i].Lease(10 * time.Second)
		require.NoError(t, err)
	}
}

func TestDMap_Lock_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	var lockContext []*LockContext
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		ctx, err := dm.Lock(key, time.Second)
		require.NoError(t, err)
		lockContext = append(lockContext, ctx)
	}

	cluster.AddMember(nil)
	for _, ctx := range lockContext {
		err = ctx.Unlock()
		require.NoError(t, err)
	}
}

func TestDMap_LockWithTimeout_ErrLockNotAcquired_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err := dm.LockWithTimeout(key, time.Second, time.Second)
		require.NoError(t, err)
	}

	cluster.AddMember(nil)

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.LockWithTimeout(key, time.Second, time.Millisecond)
		require.ErrorIs(t, err, ErrLockNotAcquired)
	}
}

func TestDMap_Lock_After_LockWithTimeout_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.LockWithTimeout(key, time.Millisecond, time.Second)
		require.NoError(t, err)
	}

	cluster.AddMember(nil)
	for i := 0; i < 100; i++ {
		key := "lock.test.foo." + strconv.Itoa(i)
		_, err = dm.Lock(key, time.Second)
		require.NoError(t, err)
	}
}

func TestDMap_tryLock(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	_, err = dm.LockWithTimeout(key, time.Second, time.Second)
	require.NoError(t, err)

	var i int
	var acquired bool
	for i <= 10 {
		i++
		_, err := dm.Lock(key, 100*time.Millisecond)
		if err == ErrLockNotAcquired {
			// already acquired
			continue
		}
		require.NoError(t, err)
		// Acquired
		acquired = true
		break
	}
	if !acquired {
		t.Fatal("Failed to acquire lock")
	}
}

func TestDMap_lockCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := resp.NewLock("lock.test", "lock.test.foo", 1).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	cmdUnlock := resp.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	require.NoError(t, err)

	err = cmdUnlock.Err()
	require.NoError(t, err)
}

func TestDMap_lockCommandHandler_EX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := resp.NewLock("lock.test", "lock.test.foo", 1).SetEX(1).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	<-time.After(2 * time.Second)

	cmdUnlock := resp.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	err = resp.ConvertError(err)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_lockCommandHandler_PX(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	cmd := resp.NewLock("lock.test", "lock.test.foo", 1).
		SetPX((10 * time.Millisecond).Milliseconds()).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err := rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	token, err := cmd.Bytes()
	require.NoError(t, err)

	<-time.After(20 * time.Millisecond)

	cmdUnlock := resp.NewUnlock("lock.test", "lock.test.foo", string(token)).Command(s.ctx)
	err = rc.Process(s.ctx, cmdUnlock)
	err = resp.ConvertError(err)
	require.ErrorIs(t, err, ErrNoSuchLock)
}

func TestDMap_lockLeaseCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout(key, time.Second, time.Second)
	require.NoError(t, err)

	// Update the timeout
	token := hex.EncodeToString(ctx.token)
	cmd := resp.NewLockLease("lock.test", "lock.test.foo", token, 10).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(2 * time.Second)

	err = ctx.Unlock()
	require.NoError(t, err)
}

func TestDMap_plockLeaseCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	key := "lock.test.foo"
	dm, err := s.NewDMap("lock.test")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout(key, 250*time.Millisecond, time.Second)
	require.NoError(t, err)

	// Update the timeout
	token := hex.EncodeToString(ctx.token)
	cmd := resp.NewPLockLease("lock.test", "lock.test.foo", token, 2000).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(500 * time.Millisecond)

	err = ctx.Unlock()
	require.NoError(t, err)
}
