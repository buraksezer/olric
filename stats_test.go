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
	"testing"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/stats"
	"github.com/stretchr/testify/require"
)

func TestOlric_Stats(t *testing.T) {
	db := newTestOlric(t)

	dm, err := db.NewDMap("mymap")
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		require.NoError(t, err)
	}

	s, err := db.Stats()
	require.NoError(t, err)

	if s.ClusterCoordinator.ID != db.rt.This().ID {
		t.Fatalf("Expected cluster coordinator: %v. Got: %v", db.rt.This(), s.ClusterCoordinator)
	}

	require.Equal(t, s.Member.Name, db.rt.This().Name)
	require.Equal(t, s.Member.ID, db.rt.This().ID)
	require.Equal(t, s.Member.Birthdate, db.rt.This().Birthdate)
	if s.Runtime != nil {
		t.Error("Runtime stats must not be collected by default:", s.Runtime)
	}

	var total int
	for partID, part := range s.Partitions {
		total += part.Length
		if _, ok := part.DMaps["mymap"]; !ok {
			t.Fatalf("Expected dmap check result is true. Got false")
		}
		if len(part.PreviousOwners) != 0 {
			t.Fatalf("Expected PreviosOwners list is empty. "+
				"Got: %v for PartID: %d", part.PreviousOwners, partID)
		}
		if part.Length <= 0 {
			t.Fatalf("Unexpected Length: %d", part.Length)
		}
	}
	if total != 100 {
		t.Fatalf("Expected total length of partition in stats is 100. Got: %d", total)
	}
	_, ok := s.ClusterMembers[stats.MemberID(db.rt.This().ID)]
	if !ok {
		t.Fatalf("Expected member ID: %d could not be found in ClusterMembers", db.rt.This().ID)
	}
}

func TestOlric_Stats_CollectRuntime(t *testing.T) {
	db := newTestOlric(t)

	s, err := db.Stats(CollectRuntime())
	require.NoError(t, err)

	if s.Runtime == nil {
		t.Fatal("Runtime stats must be collected by default:", s.Runtime)
	}
}
