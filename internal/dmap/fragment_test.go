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

package dmap

import (
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/testcluster"
	"testing"
)

func TestDMap_Fragment(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	dm, err := s.NewDMap("mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("loadFragmentFromPartition", func(t *testing.T) {
		part := s.primary.PartitionById(1)
		_, err = dm.loadFragmentFromPartition(part, "foobar")
		if err != errFragmentNotFound {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("createFragmentOnPartition", func(t *testing.T) {
		part := s.primary.PartitionById(1)
		_, err = dm.createFragmentOnPartition(part, "foobar")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	t.Run("getFragment -- errFragmentNotFound", func(t *testing.T) {
		_, err = dm.getFragment("foobar", 123, partitions.PRIMARY)
		if err != errFragmentNotFound {
			t.Fatalf("Expected %v. Got: %v", errFragmentNotFound, err)
		}
	})

	t.Run("getOrCreateFragment", func(t *testing.T) {
		_, err = dm.getOrCreateFragment("mydmap", 123, partitions.PRIMARY)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		_, err = dm.getFragment("mydmap", 123, partitions.PRIMARY)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})
}