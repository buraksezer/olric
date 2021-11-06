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

package kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/require"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func testKVStore(c *storage.Config) (storage.Engine, error) {
	kv := &KVStore{}
	if c == nil {
		c = DefaultConfig()
	}
	kv.SetConfig(c)
	child, err := kv.Fork(nil)
	if err != nil {
		return nil, err
	}

	err = child.Start()
	if err != nil {
		return nil, err
	}
	return child, nil
}

func Test_Put(t *testing.T) {
	s, err := testKVStore(nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTTL(int64(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}
}

func Test_Get(t *testing.T) {
	s, err := testKVStore(nil)
	require.NoError(t, err)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		require.NoError(t, err)

		if e.Key() != bkey(i) {
			t.Fatalf("Expected %s. Got %s", bkey(i), e.Key())
		}
		if e.TTL() != int64(i) {
			t.Fatalf("Expected %d. Got %v", i, e.TTL())
		}
		if !bytes.Equal(e.Value(), bval(i)) {
			t.Fatalf("value is malformed for %d", i)
		}
		if timestamp != e.Timestamp() {
			t.Fatalf("Expected timestamp: %d. Got: %d", timestamp, e.Timestamp())
		}
	}
}

func Test_Delete(t *testing.T) {
	s, err := testKVStore(nil)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		require.NoError(t, err)

		_, err = s.Get(hkey)
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	for _, tb := range s.(*KVStore).tables {
		s := tb.Stats()
		if s.Inuse != 0 {
			t.Fatal("inuse is different than 0.")
		}
		if s.Length != 0 {
			t.Fatalf("Expected key count is zero. Got: %d", s.Length)
		}
	}
}

func Test_ExportImport(t *testing.T) {
	timestamp := time.Now().UnixNano()
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	for i := 0; i < 1000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	fresh, err := testKVStore(nil)
	require.NoError(t, err)

	ti := s.TransferIterator()
	for ti.Next() {
		data, err := ti.Export()
		require.NoError(t, err)

		err = fresh.Import(data, func(u uint64, e storage.Entry) error {
			return fresh.Put(u, e)
		})
		require.NoError(t, err)

		err = ti.Pop()
		require.NoError(t, err)
	}

	_, err = ti.Export()
	require.ErrorIs(t, err, io.EOF)

	for i := 0; i < 1000; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := fresh.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if e.Key() != bkey(i) {
			t.Fatalf("Expected %s. Got %s", bkey(i), e.Key())
		}
		if e.TTL() != int64(i) {
			t.Fatalf("Expected %d. Got %v", i, e.TTL())
		}
		if !bytes.Equal(e.Value(), bval(i)) {
			t.Fatalf("value is malformed for %d", i)
		}
		if timestamp != e.Timestamp() {
			t.Fatalf("Expected timestamp: %d. Got: %d", timestamp, e.Timestamp())
		}
	}
}

func Test_Len(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	if s.Stats().Length != 100 {
		t.Fatalf("Expected length: 100. Got: %d", s.Stats().Length)
	}
}

func Test_Range(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		hkeys[hkey] = struct{}{}
	}

	s.Range(func(hkey uint64, entry storage.Entry) bool {
		if _, ok := hkeys[hkey]; !ok {
			t.Fatalf("Invalid hkey: %d", hkey)
		}
		return true
	})
}

func Test_Check(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		hkeys[hkey] = struct{}{}
	}

	for hkey := range hkeys {
		if !s.Check(hkey) {
			t.Fatalf("hkey could not be found: %d", hkey)
		}
	}
}

func Test_UpdateTTL(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(10)
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.UpdateTTL(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		e, err := s.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if e.Key() != bkey(i) {
			t.Fatalf("Expected key: %s. Got %s", bkey(i), e.Key())
		}
		if e.TTL() != 10 {
			t.Fatalf("Expected ttl: %d. Got %v", i, e.TTL())
		}
	}
}

func Test_GetKey(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))
	hkey := xxhash.Sum64([]byte(e.Key()))
	err = s.Put(hkey, e)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	key, err := s.GetKey(hkey)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if key != bkey(1) {
		t.Fatalf("Expected %s. Got %v", bkey(1), key)
	}
}

func Test_PutRawGetRaw(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	value := []byte("value")
	hkey := xxhash.Sum64([]byte("key"))
	err = s.PutRaw(hkey, value)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	rawval, err := s.GetRaw(hkey)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if bytes.Equal(value, rawval) {
		t.Fatalf("Expected %s. Got %v", value, rawval)
	}
}

func Test_GetTTL(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	e := entry.New()
	e.SetKey(bkey(1))
	e.SetTTL(int64(1))
	e.SetValue(bval(1))

	hkey := xxhash.Sum64([]byte(e.Key()))
	err = s.Put(hkey, e)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	ttl, err := s.GetTTL(hkey)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	if ttl != e.TTL() {
		t.Fatalf("Expected TTL %d. Got %d", ttl, e.TTL())
	}
}

func TestStorage_MatchOnKey(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	hkeys := make(map[uint64]struct{})
	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}

		e := entry.New()
		e.SetKey(key)
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		hkeys[hkey] = struct{}{}
	}

	var count int
	err = s.RegexMatchOnKeys("even:", func(hkey uint64, entry storage.Entry) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	if count != 50 {
		t.Fatalf("Expected count is 50. Got: %d", count)
	}
}

func Test_Fork(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	timestamp := time.Now().UnixNano()
	for i := 0; i < 10; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue(bval(i))
		e.SetTimestamp(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	child, err := s.Fork(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		_, err = child.Get(hkey)
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Expected storage.ErrKeyNotFound. Got %v", err)
		}
	}

	stats := child.Stats()
	if uint32(stats.Allocated) != defaultTableSize {
		t.Fatalf("Expected Stats.Allocated: %d. Got: %d", defaultTableSize, stats.Allocated)
	}

	if stats.Inuse != 0 {
		t.Fatalf("Expected Stats.Inuse: 0. Got: %d", stats.Inuse)
	}

	if stats.Garbage != 0 {
		t.Fatalf("Expected Stats.Garbage: 0. Got: %d", stats.Garbage)
	}

	if stats.Length != 0 {
		t.Fatalf("Expected Stats.Length: 0. Got: %d", stats.Length)
	}

	if stats.NumTables != 1 {
		t.Fatalf("Expected Stats.NumTables: 1. Got: %d", stats.NumTables)
	}
}

func TestKVStore_StateChange(t *testing.T) {
	s, err := testKVStore(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	timestamp := time.Now().UnixNano()
	// Current free space is 1 MB. Trigger a compaction operation.
	for i := 0; i < 100000; i++ {
		e := entry.New()
		e.SetKey(bkey(i))
		e.SetTTL(int64(i))
		e.SetValue([]byte(fmt.Sprintf("%01000d", i)))
		e.SetTTL(timestamp)
		hkey := xxhash.Sum64([]byte(e.Key()))
		err := s.Put(hkey, e)
		require.NoError(t, err)
	}

	for i, tb := range s.(*KVStore).tables {
		if tb.State() == table.ReadWriteState {
			require.Equalf(t, len(s.(*KVStore).tables)-1, i, "Writable table has to be the latest table")
		} else if tb.State() == table.ReadOnlyState {
			require.True(t, i < len(s.(*KVStore).tables)-1)
		}
	}
}
