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

package olric

import (
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/testutil/assert"
	"github.com/buraksezer/olric/query"
)

func TestOlric_DMap_Get(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	t.Run("ErrKeyNotFound", func(t *testing.T) {
		_, err = dm.Get("mykey")
		assert.Error(t, ErrKeyNotFound, err)
	})

	t.Run("Put and Get", func(t *testing.T) {
		value := "myvalue"
		err = dm.Put("mykey-2", value)
		assert.NoError(t, err)

		retrieved, err := dm.Get("mykey-2")
		assert.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})
}

func TestOlric_DMap_GetEntry(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	key := "mykey"
	value := "myvalue"
	err = dm.Put(key, value)
	assert.NoError(t, err)

	retrieved, err := dm.GetEntry(key)
	assert.NoError(t, err)
	assert.Equal(t, key, retrieved.Key)
	assert.Equal(t, value, retrieved.Value)
	assert.NotEqual(t, 0, retrieved.Timestamp)
	assert.Equal(t, int64(0), retrieved.TTL)
}

func TestOlric_DMap_Put(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	assert.NoError(t, err)
}

func TestOlric_DMap_PutIf_IfNotFound(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.PutIf("mykey", "myvalue", IfNotFound)
	assert.NoError(t, err)

	value, err := dm.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", value)

	err = dm.PutIf("mykey", "myvalue", IfNotFound)
	assert.Error(t, ErrKeyFound, err)
}

func TestOlric_DMap_PutIf_IfFound(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.PutIf("mykey", "myvalue", IfFound)
	assert.Error(t, ErrKeyNotFound, err)
}

func TestOlric_DMap_PutIfEx_IfFound(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue", time.Second, IfFound)
	assert.Error(t, err, ErrKeyNotFound)

	err = dm.Put("mykey", "myvalue")
	assert.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue-2", 100*time.Millisecond, IfFound)
	assert.NoError(t, err)

	value, err := dm.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue-2", value)

	<-time.After(100 * time.Millisecond)
	_, err = dm.Get("mykey")
	assert.Error(t, err, ErrKeyNotFound)
}

func TestOlric_DMap_PutIfEx_IfNotFound(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.PutIfEx("mykey", "myvalue", 100*time.Millisecond, IfNotFound)
	assert.NoError(t, err)

	value, err := dm.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", value)

	<-time.After(100 * time.Millisecond)
	err = dm.PutIfEx("mykey", "myvalue", 100*time.Millisecond, IfNotFound)
	assert.NoError(t, err)
}

func TestOlric_DMap_Expire(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	assert.NoError(t, err)

	err = dm.Expire("mykey", 100*time.Millisecond)
	assert.NoError(t, err)

	_, err = dm.Get("mykey")
	assert.NoError(t, err)
}

func TestOlric_DMap_Query(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	for i := 0; i < 10; i++ {
		var key string
		if i%2 == 0 {
			key = fmt.Sprintf("even:%d", i)
		} else {
			key = fmt.Sprintf("odd:%d", i)
		}
		err = dm.Put(key, "myvalue")
		assert.NoError(t, err)
	}

	c, err := dm.Query(query.M{
		"$onKey": query.M{
			"$regexMatch": "^even:",
		},
	})
	assert.NoError(t, err)

	defer c.Close()
	expected := map[string]string{
		"even:8": "myvalue",
		"even:0": "myvalue",
		"even:6": "myvalue",
		"even:2": "myvalue",
		"even:4": "myvalue",
	}
	var count int
	err = c.Range(func(key string, value interface{}) bool {
		val, ok := expected[key]
		assert.Equal(t, true, ok)
		assert.Equal(t, val, value)
		count++
		return true
	})
	assert.NoError(t, err)
	assert.Equal(t, len(expected), count)
}

func TestOlric_DMap_PutEx(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.PutEx("mykey", "myvalue", 100*time.Millisecond)
	assert.NoError(t, err)

	value, err := dm.Get("mykey")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", value)

	<-time.After(100 * time.Millisecond)
	_, err = dm.Get("mykey")
	assert.Error(t, ErrKeyNotFound, err)
}

func TestOlric_DMap_Delete(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.Put("mykey", "myvalue")
	assert.NoError(t, err)

	err = dm.Delete("mykey")
	assert.NoError(t, err)

	_, err = dm.Get("mykey")
	assert.Error(t, ErrKeyNotFound, err)
}

func TestOlric_DMap_Incr(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	value, err := dm.Incr("mykey", 10)
	assert.NoError(t, err)
	assert.Equal(t, 10, value)
}

func TestOlric_DMap_Decr(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	value, err := dm.Decr("mykey", 10)
	assert.NoError(t, err)
	assert.Equal(t, -10, value)
}

func TestOlric_DMap_GetPut(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	key := "mykey"
	value := "myvalue"
	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.Put(key, value)
	assert.NoError(t, err)

	oldval, err := dm.GetPut(key, "new-value")
	assert.NoError(t, err)
	assert.Equal(t, value, oldval)

	current, err := dm.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, "new-value", current)
}

func TestOlric_DMap_Destroy(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	key := "mykey"
	value := "myvalue"
	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	err = dm.Put(key, value)
	assert.NoError(t, err)

	err = dm.Destroy()
	assert.NoError(t, err)

	_, err = dm.Get(key)
	assert.Error(t, ErrKeyNotFound, err)
}

func TestOlric_DMap_LockWithTimeout(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	ctx, err := dm.LockWithTimeout("mykey", time.Second, time.Second)
	assert.NoError(t, err)

	err = ctx.Unlock()
	assert.NoError(t, err)
}

func TestOlric_DMap_LockWithTimeout_Timeout(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	_, err = dm.LockWithTimeout("mykey", 100*time.Millisecond, time.Second)
	assert.NoError(t, err)

	err = testutil.TryWithInterval(10, time.Millisecond, func() error {
		_, err = dm.LockWithTimeout("mykey", 100*time.Millisecond, time.Millisecond)
		return err
	})
	assert.Error(t, ErrLockNotAcquired, err)
}

func TestOlric_DMap_Lock(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	ctx, err := dm.Lock("mykey", time.Second)
	assert.NoError(t, err)

	err = ctx.Unlock()
	assert.NoError(t, err)
}

func TestOlric_DMap_Lock_Deadline(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mydmap")
	assert.NoError(t, err)

	ctx, err := dm.Lock("mykey", time.Second)
	assert.NoError(t, err)

	defer func() {
		err = ctx.Unlock()
		assert.NoError(t, err)
	}()

	_, errTwo := dm.Lock("mykey", time.Millisecond)
	assert.Error(t, ErrLockNotAcquired, errTwo)
}
