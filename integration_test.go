// Copyright 2018-2022 Burak Sezer
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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/stretchr/testify/require"
)

func TestIntegration_NodesJoinOrLeftDuringQuery(t *testing.T) {
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 2
		c.WriteQuorum = 1
		c.ReadRepair = true
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())
	db2 := cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for 1 second before inserting keys")
	<-time.After(time.Second)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < 100000; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
		if i == 5999 {
			go cluster.addMemberWithConfig(t, newConfig())
		}
	}

	go cluster.addMemberWithConfig(t, newConfig())

	t.Log("Fetch all keys")

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err)
		if i == 5999 {
			err = c.client.Close(db2.name)
			require.NoError(t, err)

			t.Logf("Shutdown one of the nodes: %s", db2.name)
			require.NoError(t, db2.Shutdown(ctx))

			go cluster.addMemberWithConfig(t, newConfig())

			t.Log("Wait for \"NodeLeave\" event propagation")
			<-time.After(time.Second)
		}
	}

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err)
	}
}

func TestIntegration_DMap_Cache_Eviction_LRU_MaxKeys(t *testing.T) {
	var maxKeys = 100000
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxKeys = maxKeys
		c.DMaps.EvictionPolicy = config.LRUEviction
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	var total int
	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue", NX())
		if err == ErrKeyFound {
			err = nil
		} else {
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
	t.Logf("number of misses: %d, utilization rate: %f", total, float64(100)-(float64(total*100))/float64(maxKeys))
}

func TestIntegration_DMap_Cache_Eviction_MaxKeys(t *testing.T) {
	var maxKeys = 100000
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxKeys = maxKeys
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	var total int
	for i := maxKeys; i < 2*maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue", NX())
		if err == ErrKeyFound {
			err = nil
		} else {
			total++
		}
		require.NoError(t, err)
	}

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Equal(t, maxKeys, total)
}

func TestIntegration_DMap_Cache_Eviction_MaxIdleDuration(t *testing.T) {
	var maxKeys = 100000
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxIdleDuration = 100 * time.Millisecond
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
}

func TestIntegration_DMap_Cache_Eviction_TTLDuration(t *testing.T) {
	var maxKeys = 100000
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.TTLDuration = 100 * time.Millisecond
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Equal(t, maxKeys, total)
}

func TestIntegration_DMap_Cache_Eviction_LRU_MaxInuse(t *testing.T) {
	var maxKeys = 100000
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 1
		c.WriteQuorum = 1
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		c.DMaps.MaxInuse = 100 // bytes
		c.DMaps.EvictionPolicy = "LRU"
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	for i := 0; i < maxKeys; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	<-time.After(250 * time.Millisecond)

	var total int

	for i := 0; i < maxKeys; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			err = nil
			total++
		}
		require.NoError(t, err)
	}
	require.Greater(t, total, 0)
}

func TestIntegration_Kill_Nodes_During_Operation(t *testing.T) {
	newConfig := func() *config.Config {
		c := config.New("local")
		c.PartitionCount = config.DefaultPartitionCount
		c.ReplicaCount = 3
		c.WriteQuorum = 1
		c.ReadRepair = true
		c.ReadQuorum = 1
		c.LogOutput = io.Discard
		require.NoError(t, c.Sanitize())
		require.NoError(t, c.Validate())
		return c
	}

	cluster := newTestOlricCluster(t)

	db := cluster.addMemberWithConfig(t, newConfig())

	cluster.addMemberWithConfig(t, newConfig())
	db3 := cluster.addMemberWithConfig(t, newConfig())
	cluster.addMemberWithConfig(t, newConfig())
	db5 := cluster.addMemberWithConfig(t, newConfig())

	t.Log("Wait for 1 second before inserting keys")
	<-time.After(time.Second)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	require.NoError(t, err)

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	t.Log("Insert keys")

	for i := 0; i < 100000; i++ {
		err = dm.Put(ctx, fmt.Sprintf("mykey-%d", i), "myvalue")
		require.NoError(t, err)
	}

	t.Log("Fetch all keys")

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(ctx, fmt.Sprintf("mykey-%d", i))
		if err == ErrKeyNotFound {
			fmt.Println(i)
			err = nil
		}
		require.NoError(t, err)
	}

	t.Logf("Terminate %s", db3.rt.This())
	require.NoError(t, db3.Shutdown(context.Background()))

	t.Logf("Terminate %s", db5.rt.This())
	require.NoError(t, db5.Shutdown(context.Background()))

	t.Log("Wait for \"NodeLeave\" event propagation")
	<-time.After(time.Second)

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err)
	}
}
