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
	"golang.org/x/sync/errgroup"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedClient_NewDMap(t *testing.T) {
	db := newTestOlric(t)

	e := db.NewEmbeddedClient()
	_, err := e.NewDMap("mydmap")
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Put(t *testing.T) {
	db := newTestOlric(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)
}

func TestEmbeddedClient_DMap_Get(t *testing.T) {
	db := newTestOlric(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)

	gr, err := dm.Get(context.Background(), "mykey")
	require.NoError(t, err)

	value, err := gr.String()
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)
}

func TestEmbeddedClient_DMap_Delete(t *testing.T) {
	db := newTestOlric(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(context.Background(), "mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Delete(context.Background(), "mykey")
	require.NoError(t, err)

	_, err = dm.Get(context.Background(), "mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestEmbeddedClient_DMap_Atomic_Incr(t *testing.T) {
	db := newTestOlric(t)

	e := db.NewEmbeddedClient()
	dm, err := e.NewDMap("mydmap")
	require.NoError(t, err)

	var errGr errgroup.Group
	for i := 0; i < 100; i++ {
		errGr.Go(func() error {
			_, err = dm.Incr("mykey", 1)
			return err
		})
	}
	require.NoError(t, errGr.Wait())

	gr, err := dm.Get(context.Background(), "mykey")
	res, err := gr.Int()
	require.NoError(t, err)
	require.Equal(t, 100, res)

}
