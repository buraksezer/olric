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
	"testing"

	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_AccessLog(t *testing.T) {
	a := newAccessLog()
	partitions.SetHashFunc(hasher.NewDefaultHasher())

	hkey := partitions.HKey("mydmap", "mykey")
	a.touch(hkey)

	t.Run("get", func(t *testing.T) {
		ts, ok := a.get(hkey)
		if !ok {
			t.Fatalf("Expected true. Got: %v", ok)
		}
		if ts == 0 {
			t.Fatalf("Invalid timestamp: %v", ts)
		}
	})

	t.Run("delete", func(t *testing.T) {
		ts, ok := a.get(hkey)
		if !ok {
			t.Fatalf("Expected true. Got: %v", ok)
		}
		if ts == 0 {
			t.Fatalf("Invalid timestamp: %v", ts)
		}
	})

	t.Run("iterator", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			hkey = partitions.HKey("mydmap", testutil.ToKey(i))
			a.touch(hkey)
		}

		var counter int
		a.iterator(func(hkey uint64, timestamp int64) bool {
			if hkey == 0 {
				t.Fatalf("HKey cannot be zero")
			}
			if timestamp == 0 {
				t.Fatalf("Timestamp cannot be zero")
			}
			counter++
			return true
		})
		if counter != 11 {
			t.Fatalf("Expected item count is 11. Got: %v", counter)
		}
	})
}
