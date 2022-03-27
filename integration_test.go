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
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/testutil"
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

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	db2 := cluster.addMemberWithConfig(t, newConfig())

	err = testutil.TryWithInterval(100, 50*time.Millisecond, func() error {
		routes, err := c.RoutingTable(ctx)
		if err != nil {
			return err
		}
		// Wait for the second node.
		for _, route := range routes {
			if len(route.ReplicaOwners) == 0 {
				return errors.New("not ready for clustering test")
			}
		}
		return nil
	})
	require.NoError(t, err)

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
		}
	}

	for i := 0; i < 100000; i++ {
		_, err = dm.Get(context.Background(), fmt.Sprintf("mykey-%d", i))
		require.NoError(t, err)
	}
}
