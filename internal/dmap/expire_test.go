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
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDMap_Expire(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(key, "myvalue")
	require.NoError(t, err)

	_, err = dm.Get(key)
	require.NoError(t, err)

	err = dm.Expire(key, time.Millisecond)
	require.NoError(t, err)

	<-time.After(time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_ErrKeyNotFound(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Expire("mykey", time.Millisecond)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_expireCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(key, "myvalue")
	require.NoError(t, err)

	cmd := resp.NewExpire("mydmap", "mykey", time.Duration(0.1*float64(time.Second))).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(200 * time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestDMap_Expire_pexpireCommandHandler(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mydmap")
	require.NoError(t, err)

	key := "mykey"
	err = dm.Put(key, "myvalue")
	require.NoError(t, err)

	cmd := resp.NewPExpire("mydmap", "mykey", time.Millisecond).Command(s.ctx)
	rc := s.respClient.Get(s.rt.This().String())
	err = rc.Process(s.ctx, cmd)
	require.NoError(t, err)

	<-time.After(10 * time.Millisecond)

	// Get the value and check it.
	_, err = dm.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}
