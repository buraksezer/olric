// Copyright 2018-2020 Burak Sezer
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
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash"
)

var storageTestLock sync.RWMutex

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func Test_Put(t *testing.T) {
	s := New(0)

	for i := 0; i < 100; i++ {
		entry := NewEntry()
		entry.SetKey(bkey(i))
		entry.SetValue(bval(i))
		entry.SetTTL(int64(i))
		entry.SetTimestamp(time.Now().UnixNano())
		hkey := xxhash.Sum64([]byte(entry.Key()))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}
}

func Test_Get(t *testing.T) {
	s := New(0)

	timestamp := time.Now().UnixNano()
	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: timestamp,
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		entry, err := s.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if entry.Key != bkey(i) {
			t.Fatalf("Expected %s. Got %s", bkey(i), entry.Key)
		}
		if entry.TTL != int64(i) {
			t.Fatalf("Expected %d. Got %v", i, entry.TTL)
		}
		if !bytes.Equal(entry.Value, bval(i)) {
			t.Fatalf("value is malformed for %d", i)
		}
		if timestamp != entry.Timestamp {
			t.Fatalf("Expected Timestamp: %d. Got: %d", timestamp, entry.Timestamp)
		}
	}
}

func Test_Delete(t *testing.T) {
	s := New(0)

	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		err := s.Delete(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		_, err = s.Get(hkey)
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	for _, item := range s.tables {
		if item.inuse != 0 {
			t.Fatal("inuse is different than 0.")
		}
		if len(item.hkeys) != 0 {
			t.Fatalf("Expected key count is zero. Got: %d", len(item.hkeys))
		}
	}
}

func Test_CompactTables(t *testing.T) {
	s := New(0)

	compaction := func() {
		storageTestLock.Lock()
		defer storageTestLock.Unlock()
		for {
			if done := s.CompactTables(); done {
				return
			}
		}
	}
	timestamp := time.Now().UnixNano()
	// Current free space is 1MB. Trigger a compaction operation.
	for i := 0; i < 1500; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     []byte(fmt.Sprintf("%01000d", i)),
			Timestamp: timestamp,
		}
		hkey := xxhash.Sum64([]byte(entry.Key))

		storageTestLock.Lock()
		err := s.Put(hkey, entry)
		storageTestLock.Unlock()

		if err == ErrFragmented {
			go compaction()
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 1500; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))

		storageTestLock.RLock()
		entry, err := s.Get(hkey)
		storageTestLock.RUnlock()

		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if entry.Key != bkey(i) {
			t.Fatalf("Expected %s. Got %s", bkey(i), entry.Key)
		}
		if entry.TTL != int64(i) {
			t.Fatalf("Expected %d. Got %v", i, entry.TTL)
		}
		if timestamp != entry.Timestamp {
			t.Fatalf("Expected Timestamp: %d. Got: %d", timestamp, entry.Timestamp)
		}
		val := []byte(fmt.Sprintf("%01000d", i))
		if !bytes.Equal(entry.Value, val) {
			t.Fatalf("value is malformed for %d", i)
		}
	}

	for i := 0; i < 10; i++ {
		storageTestLock.Lock()
		if len(s.tables) == 1 {
			storageTestLock.Unlock()
			// It's OK.
			return
		}
		storageTestLock.Unlock()
		<-time.After(100 * time.Millisecond)
	}
	t.Error("Tables cannot be compacted.")
}

func Test_PurgeTables(t *testing.T) {
	s := New(0)

	var isFragmented bool

	compaction := func() {
		storageTestLock.Lock()
		defer storageTestLock.Unlock()
		for {
			if done := s.CompactTables(); done {
				return
			}
		}
	}
	timestamp := time.Now().UnixNano()
	// Current free space is 65kb. Trigger a compaction operation.
	for i := 0; i < 2000; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     []byte(fmt.Sprintf("%01000d", i)),
			Timestamp: timestamp,
		}
		hkey := xxhash.Sum64([]byte(entry.Key))

		// Simulate the real-world case.
		storageTestLock.Lock()
		err := s.Put(hkey, entry)
		storageTestLock.Unlock()

		if err == ErrFragmented {
			isFragmented = true
			go compaction()
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 2000; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))

		// Simulate the real-world case.
		storageTestLock.Lock()
		err := s.Delete(hkey)
		storageTestLock.Unlock()
		if err == ErrFragmented {
			isFragmented = true
			go compaction()
			err = nil
		}
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 10; i++ {
		storageTestLock.Lock()
		if isFragmented && len(s.tables) == 1 {
			storageTestLock.Unlock()
			return
		}
		storageTestLock.Unlock()
		<-time.After(100 * time.Millisecond)
	}
	t.Fatal("Tables cannot be purged.")
}

func Test_ExportImport(t *testing.T) {
	timestamp := time.Now().UnixNano()
	s := New(0)
	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: timestamp,
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}
	data, err := s.Export()
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	fresh, err := Import(data)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		entry, err := fresh.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if entry.Key != bkey(i) {
			t.Fatalf("Expected %s. Got %s", bkey(i), entry.Key)
		}
		if entry.TTL != int64(i) {
			t.Fatalf("Expected %d. Got %v", i, entry.TTL)
		}
		if !bytes.Equal(entry.Value, bval(i)) {
			t.Fatalf("value is malformed for %d", i)
		}
		if timestamp != entry.Timestamp {
			t.Fatalf("Expected Timestamp: %d. Got: %d", timestamp, entry.Timestamp)
		}
	}
}

func Test_Len(t *testing.T) {
	s := New(0)
	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:   bkey(i),
			TTL:   int64(i),
			Value: bval(i),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	if s.Len() != 100 {
		t.Fatalf("Expected length: 100. Got: %d", s.Len())
	}
}

func Test_Range(t *testing.T) {
	s := New(0)
	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		hkeys[hkey] = struct{}{}
	}

	s.Range(func(hkey uint64, entry *Entry) bool {
		if _, ok := hkeys[hkey]; !ok {
			t.Fatalf("Invalid hkey: %d", hkey)
		}
		return true
	})
}

func Test_Check(t *testing.T) {
	s := New(0)
	hkeys := make(map[uint64]struct{})
	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
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
	s := New(0)

	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			Value:     bval(i),
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		entry := &Entry{
			key:       bkey(i),
			TTL:       10,
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.UpdateTTL(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		hkey := xxhash.Sum64([]byte(bkey(i)))
		entry, err := s.Get(hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		if entry.Key != bkey(i) {
			t.Fatalf("Expected key: %s. Got %s", bkey(i), entry.Key)
		}
		if entry.TTL != 10 {
			t.Fatalf("Expected TTL: %d. Got %v", i, entry.TTL)
		}
	}
}

func Test_GetKey(t *testing.T) {
	s := New(0)
	entry := &Entry{
		key:   bkey(1),
		TTL:   int64(1),
		Value: bval(1),
	}
	hkey := xxhash.Sum64([]byte(entry.Key))
	err := s.Put(hkey, entry)
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
	s := New(0)
	value := []byte("value")
	hkey := xxhash.Sum64([]byte("key"))
	err := s.PutRaw(hkey, value)
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
	s := New(0)
	entry := &Entry{
		key:   bkey(1),
		TTL:   int64(1),
		Value: bval(1),
	}
	hkey := xxhash.Sum64([]byte(entry.Key))
	err := s.Put(hkey, entry)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	ttl, err := s.GetTTL(hkey)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}

	if ttl != entry.TTL {
		t.Fatalf("Expected TTL %d. Got %d", ttl, entry.TTL)
	}
}

func TestStorage_MatchOnKey(t *testing.T) {
	s := New(0)
	hkeys := make(map[uint64]struct{})
	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}

		entry := &Entry{
			key:       key,
			TTL:       int64(i),
			Value:     bval(i),
			Timestamp: time.Now().UnixNano(),
		}
		hkey := xxhash.Sum64([]byte(entry.Key))
		err := s.Put(hkey, entry)
		if err != nil {
			t.Fatalf("Expected nil. Got %v", err)
		}
		hkeys[hkey] = struct{}{}
	}

	var count int
	err := s.MatchOnKey("even:", func(hkey uint64, entry *Entry) bool {
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
