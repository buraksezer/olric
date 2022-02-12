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
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestOlric_DMap_Get(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	t.Run("ErrKeyNotFound", func(t *testing.T) {
		_, err = dm.Get("mykey")
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("Put and Get", func(t *testing.T) {
		value := "myvalue"
		err = dm.Put("mykey-2", value)
		require.NoError(t, err)

		retrieved, err := dm.Get("mykey-2")
		require.NoError(t, err)
		require.Equal(t, value, retrieved)
	})
}

func TestOlric_DMap_Put(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	require.NoError(t, err)
}

/*
func TestOlric_DMap_PutIf_IfNotFound(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put("mykey", "myvalue", IfNotFound)
	require.NoError(t, err)

	value, err := dm.Get("mykey")
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)

	err = dm.PutIf("mykey", "myvalue", IfNotFound)
	require.ErrorIs(t, err, ErrKeyFound)
}

func TestOlric_DMap_PutIf_IfFound(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.PutIf("mykey", "myvalue", IfFound)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestOlric_DMap_PutIfEx_IfFound(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue", time.Second, IfFound)
	require.ErrorIs(t, err, ErrKeyNotFound)

	err = dm.Put("mykey", "myvalue")
	require.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue-2", 100*time.Millisecond, IfFound)
	require.NoError(t, err)

	value, err := dm.Get("mykey")
	require.NoError(t, err)
	require.Equal(t, "myvalue-2", value)

	<-time.After(100 * time.Millisecond)
	_, err = dm.Get("mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestOlric_DMap_PutIfEx_IfNotFound(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue", 100*time.Millisecond, IfNotFound)
	require.NoError(t, err)

	value, err := dm.Get("mykey")
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)

	<-time.After(100 * time.Millisecond)
	err = dm.PutIfEx("mykey", "myvalue", 100*time.Millisecond, IfNotFound)
	require.NoError(t, err)
}*/

func TestOlric_DMap_Expire(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Expire("mykey", 100*time.Millisecond)
	require.NoError(t, err)

	_, err = dm.Get("mykey")
	require.NoError(t, err)
}

/*
func TestOlric_DMap_PutEx(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.PutEx("mykey", "myvalue", 100*time.Millisecond)
	require.NoError(t, err)

	value, err := dm.Get("mykey")
	require.NoError(t, err)
	require.Equal(t, "myvalue", value)

	<-time.After(100 * time.Millisecond)
	_, err = dm.Get("mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}*/

func TestOlric_DMap_Delete(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	require.NoError(t, err)

	err = dm.Delete("mykey")
	require.NoError(t, err)

	_, err = dm.Get("mykey")
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestOlric_DMap_Incr(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	value, err := dm.Incr("mykey", 10)
	require.NoError(t, err)
	require.Equal(t, 10, value)
}

func TestOlric_DMap_Decr(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	value, err := dm.Decr("mykey", 10)
	require.NoError(t, err)
	require.Equal(t, -10, value)
}

func TestOlric_DMap_GetPut(t *testing.T) {
	db := newTestOlric(t)

	key := "mykey"
	value := "myvalue"
	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(key, value)
	require.NoError(t, err)

	oldval, err := dm.GetPut(key, "new-value")
	require.NoError(t, err)
	require.Equal(t, value, oldval)

	current, err := dm.Get(key)
	require.NoError(t, err)
	require.Equal(t, "new-value", current)
}

func TestOlric_DMap_Destroy(t *testing.T) {
	db := newTestOlric(t)

	key := "mykey"
	value := "myvalue"
	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	err = dm.Put(key, value)
	require.NoError(t, err)

	err = dm.Destroy()
	require.NoError(t, err)

	_, err = dm.Get(key)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestOlric_DMap_LockWithTimeout(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	ctx, err := dm.LockWithTimeout("mykey", time.Second, time.Second)
	require.NoError(t, err)

	err = ctx.Unlock()
	require.NoError(t, err)
}

func TestOlric_DMap_LockWithTimeout_Timeout(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	_, err = dm.LockWithTimeout("mykey", 100*time.Millisecond, time.Second)
	require.NoError(t, err)

	err = testutil.TryWithInterval(10, time.Millisecond, func() error {
		_, err = dm.LockWithTimeout("mykey", 100*time.Millisecond, time.Millisecond)
		return err
	})
	require.ErrorIs(t, err, ErrLockNotAcquired)
}

func TestOlric_DMap_Lock(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	ctx, err := dm.Lock("mykey", time.Second)
	require.NoError(t, err)

	err = ctx.Unlock()
	require.NoError(t, err)
}

func TestOlric_DMap_Lock_Deadline(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mydmap")
	require.NoError(t, err)

	ctx, err := dm.Lock("mykey", time.Second)
	require.NoError(t, err)

	defer func() {
		err = ctx.Unlock()
		require.NoError(t, err)
	}()

	_, errTwo := dm.Lock("mykey", time.Millisecond)
	require.ErrorIs(t, ErrLockNotAcquired, errTwo)
}
