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
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDMapPipeline_Put(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)

	futures := make(map[int]*FuturePut)
	pipe := dm.(*ClusterDMap).Pipeline()
	for i := 0; i < 100; i++ {
		fp, err := pipe.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
		futures[i] = fp
	}
	err = pipe.Flush(ctx)
	require.NoError(t, err)

	for _, fp := range futures {
		require.NoError(t, fp.Result())
	}

	for i := 0; i < 100; i++ {
		key := testutil.ToKey(i)
		gr, err := dm.Get(ctx, key)
		require.NoError(t, err)

		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

func TestDMapPipeline_Get(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe := dm.(*ClusterDMap).Pipeline()
	futures := make(map[int]*FutureGet)
	for i := 0; i < 100; i++ {
		fg := pipe.Get(ctx, testutil.ToKey(i))
		futures[i] = fg
	}

	err = pipe.Flush(ctx)
	require.NoError(t, err)

	for i, fg := range futures {
		gr, err := fg.Result()
		require.NoError(t, err)

		value, err := gr.Byte()
		require.NoError(t, err)
		require.Equal(t, testutil.ToVal(i), value)
	}
}

func TestDMapPipeline_Delete(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe := dm.(*ClusterDMap).Pipeline()
	futures := make(map[int]*FutureDelete)
	for i := 0; i < 100; i++ {
		fd := pipe.Delete(ctx, testutil.ToKey(i))
		futures[i] = fd
	}

	err = pipe.Flush(ctx)
	require.NoError(t, err)

	for _, fd := range futures {
		num, err := fd.Result()
		require.NoError(t, err)
		require.Equal(t, 1, num)
	}
}

func TestDMapPipeline_Expire(t *testing.T) {
	cluster := newTestOlricCluster(t)
	db := cluster.addMember(t)

	ctx := context.Background()
	c, err := NewClusterClient([]string{db.name})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, c.Close(ctx))
	}()

	dm, err := c.NewDMap("mydmap")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		err = dm.Put(ctx, testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	pipe := dm.(*ClusterDMap).Pipeline()
	futures := make(map[int]*FutureExpire)
	for i := 0; i < 100; i++ {
		fd, err := pipe.Expire(ctx, testutil.ToKey(i), time.Hour)
		require.NoError(t, err)
		futures[i] = fd
	}

	err = pipe.Flush(ctx)
	require.NoError(t, err)

	for _, fd := range futures {
		err := fd.Result()
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		gr, err := dm.Get(ctx, testutil.ToKey(i))
		require.NoError(t, err)
		require.NotEqual(t, int64(0), gr.TTL())
	}
}

func ExamplePipeline() {
	c, err := NewClusterClient([]string{"127.0.0.1:3320"})
	if err != nil {
		// Handle this error
	}
	dm, err := c.NewDMap("mydmap")
	if err != nil {
		// Handle this error
	}

	ctx := context.Background()

	pipe := dm.(*ClusterDMap).Pipeline()

	futurePut, err := pipe.Put(ctx, "key-1", "value-1")
	if err != nil {
		// Handle this error
	}

	err = pipe.Flush(context.Background())
	if err != nil {
		// Handle this error
	}

	err = futurePut.Result()
	if err != nil {
		// Handle this error
	}
}
