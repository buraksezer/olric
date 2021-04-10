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

package olric

import (
	"bytes"
	"testing"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testutil"
	"github.com/buraksezer/olric/internal/testutil/assert"
)

func TestOlric_Stats(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	dm, err := db.NewDMap("mymap")
	assert.NoError(t, err)

	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		assert.NoError(t, err)
	}

	s, err := db.Stats()
	assert.NoError(t, err)

	if !s.ClusterCoordinator.CompareByID(db.rt.This()) {
		t.Fatalf("Expected cluster coordinator: %v. Got: %v", db.rt.This(), s.ClusterCoordinator)
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
		if !part.Owner.CompareByID(db.rt.This()) {
			t.Fatalf("Expected partition owner: %s. Got: %s", db.rt.This(), part.Owner)
		}
	}
	if total != 100 {
		t.Fatalf("Expected total length of partition in stats is 100. Got: %d", total)
	}
}

func TestOlric_Stats_Operation(t *testing.T) {
	db, err := newTestOlric(t)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	req := protocol.NewSystemMessage(protocol.OpStats)
	req.SetBuffer(buf)
	err = req.Encode()
	assert.NoError(t, err)
	resp := req.Response(nil)
	db.statsOperation(resp, req)
	assert.Equal(t, protocol.StatusOK, resp.Status())
}
