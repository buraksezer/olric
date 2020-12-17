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

package partitions

import (
	"reflect"
	"testing"

	"github.com/buraksezer/olric/internal/discovery"
)

type testStorageUnit struct {
	length int
}

func (su *testStorageUnit) Length() int {
	return su.length
}

func TestPartition(t *testing.T) {
	p := Partition{
		id:   1,
		kind: PRIMARY,
	}

	tmp := []discovery.Member{{
		Name: "test-member",
	}}
	p.SetOwners(tmp)

	t.Run("Owners", func(t *testing.T) {
		owners := p.Owners()
		if !reflect.DeepEqual(owners, tmp) {
			t.Fatalf("Partition owners slice is different")
		}
	})

	t.Run("Owner", func(t *testing.T) {
		owner := p.Owner()
		if !reflect.DeepEqual(owner, tmp[0]) {
			t.Fatalf("Partition owners slice is different")
		}
	})

	t.Run("OwnerCount", func(t *testing.T) {
		count := p.OwnerCount()
		if count != 1 {
			t.Fatalf("Expected owner count is 1. Got: %d", count)
		}
	})

	t.Run("Length", func(t *testing.T) {
		s1 := &testStorageUnit{length: 10}
		s2 := &testStorageUnit{length: 20}
		p.Map.Store("s1", s1)
		p.Map.Store("s2", s2)
		length := p.Length()
		if length != 30 {
			t.Fatalf("Expected length: 30. Got: %d", length)
		}
	})
}
