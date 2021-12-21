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

func testScanIterator(t *testing.T, s *Service, allKeys map[string]bool, replica bool) int {
	ctx := context.TODO()
	rc := s.respClient.Get(s.rt.This().String())

	var totalKeys int
	var partID, cursor uint64
	for {
		r := resp.NewScan(partID, "mydmap", cursor)
		if replica {
			r.SetReplica()
		}
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
	return totalKeys
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

	totalKeys := testScanIterator(t, s, allKeys, false)
	require.Equal(t, 100, totalKeys)
	for _, value := range allKeys {
		require.True(t, value)
	}
}

func TestDMap_scanCommandHandler_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.WriteQuorum = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c1.WriteQuorum = 2
	e2 := testcluster.NewEnvironment(c2)
	s2 := cluster.AddMember(e2).(*Service)

	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), i)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}

	t.Run("Scan on primary copies", func(t *testing.T) {
		var totalKeys int
		totalKeys += testScanIterator(t, s1, allKeys, false)
		totalKeys += testScanIterator(t, s2, allKeys, false)

		require.Equal(t, 100, totalKeys)
		for _, value := range allKeys {
			require.True(t, value)
		}
	})

	t.Run("Scan on replicas", func(t *testing.T) {
		var totalKeys int
		totalKeys += testScanIterator(t, s1, allKeys, true)
		totalKeys += testScanIterator(t, s2, allKeys, true)

		require.Equal(t, 100, totalKeys)
		for _, value := range allKeys {
			require.True(t, value)
		}
	})
}

func TestDMap_Scan(t *testing.T) {
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
	sc, err := dm.Scan()
	require.NoError(t, err)
	err = sc.Range(func(key string) bool {
		require.Contains(t, allKeys, key)
		return true
	})
	require.NoError(t, err)
}

func TestDMap_Scan_Cluster(t *testing.T) {
	cluster := testcluster.New(NewService)

	c1 := testutil.NewConfig()
	c1.ReplicaCount = 2
	c1.WriteQuorum = 2
	e1 := testcluster.NewEnvironment(c1)
	s1 := cluster.AddMember(e1).(*Service)

	c2 := testutil.NewConfig()
	c2.ReplicaCount = 2
	c1.WriteQuorum = 2
	e2 := testcluster.NewEnvironment(c2)
	cluster.AddMember(e2)

	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mydmap")
	require.NoError(t, err)

	allKeys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), i)
		require.NoError(t, err)

		allKeys[testutil.ToKey(i)] = false
	}
	sc, err := dm.Scan()
	require.NoError(t, err)
	err = sc.Range(func(key string) bool {
		require.Contains(t, allKeys, key)
		return true
	})
	require.NoError(t, err)
}
