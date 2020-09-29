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

package compare

import (
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestCompare_Difference(t *testing.T) {
	primary := NewDataSet()
	for i := uint64(0); i < 10; i++ {
		primary.Add(i, i)
	}

	replica := NewDataSet()
	for i := uint64(0); i < 10; i++ {
		if i == 5 {
			// inconsistency
			replica.Add(i, i*2)
			continue
		}
		replica.Add(i, i)
	}

	added, deleted, err := Cmp(primary, replica)
	if err != nil {
		t.Errorf("Expected nil. Got %v", err)
	}

	for _, item := range added {
		if item.HKey != 5 {
			t.Errorf("Expected HKey is 5. Got: %d", item.HKey)
		}
		if item.Timestamp != 10 {
			t.Errorf("Expected Timestamp is 10. Got: %d", item.Timestamp)
		}
	}

	for _, item := range deleted {
		if item.HKey != 5 {
			t.Errorf("Expected HKey is 5. Got: %d", item.HKey)
		}
		if item.Timestamp != 5 {
			t.Errorf("Expected Timestamp is 5. Got: %d", item.Timestamp)
		}
	}
}

func TestCompare_ExportImport(t *testing.T) {
	one := NewDataSet()
	for i := uint64(0); i < 10; i++ {
		one.Add(i, i)
	}

	data, err := one.Export()
	if err != nil {
		t.Errorf("Expected nil. Got %v", err)
	}
	two, err := Import(data)
	if err != nil {
		t.Errorf("Expected nil. Got %v", err)
	}
	equal := cmp.Equal(one.KVItems(), two.KVItems())
	if !equal {
		t.Fatalf("Different")
	}
}
