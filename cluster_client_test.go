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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClusterClient_Ping(t *testing.T) {
	cluster := newTestOlricCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	err = c.Ping(ctx, db.rt.This().String())
	require.NoError(t, err)
}

func TestClusterClient_PingWithMessage(t *testing.T) {
	cluster := newTestOlricCluster(t)
	cluster.addMember(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	message := "Olric is the best!"
	result, err := c.PingWithMessage(ctx, db.rt.This().String(), message)
	require.NoError(t, err)
	require.Equal(t, message, result)
}

func TestClusterClient_RoutingTable(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{db.name}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	rt, err := c.RoutingTable(ctx)
	require.NoError(t, err)

	require.Len(t, rt, int(db.config.PartitionCount))
}

func TestClusterClient_Put(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{db.name}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)
}

func TestClusterClient_Get(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{db.name}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	gr, err := dm.Get(ctx, "mykey")
	require.NoError(t, err)

	res, err := gr.String()
	require.NoError(t, err)

	require.Equal(t, res, "myvalue")
}

func TestClusterClient_Delete(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{db.name}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Delete(ctx, "mykey")
	require.NoError(t, err)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestClusterClient_Destroy(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	c, err := NewClusterClient([]string{db.name}, nil)
	require.NoError(t, err)

	ctx := context.Background()
	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(ctx, "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Destroy(ctx)
	require.NoError(t, err)

	_, err = dm.Get(ctx, "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}
