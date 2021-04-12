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
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_Merge_Fragments(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()
	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var key string
	for i := 0; i < 100; i++ {
		key = "balancer-test." + strconv.Itoa(i)
		err = dm.Put(key, testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	hkey := partitions.HKey("mymap", key)
	f, err := dm.getFragment(hkey, partitions.PRIMARY)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	t.Run("Move", func(t *testing.T) {
		err = f.Move(3, partitions.PRIMARY, "mymap", s.rt.This())
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	})

	t.Run("invalid partition id", func(t *testing.T) {
		fp := &fragmentPack{
			PartID:    12312,
			Kind:      partitions.PRIMARY,
			Name:      "foobar",
			Payload:   nil,
			AccessLog: f.accessLog.m,
		}
		err = s.validateFragmentPack(fp)
		if err == nil {
			t.Fatalf("Expected an error. Got: %v", err)
		}
	})

	t.Run("extractFragmentPack", func(t *testing.T) {
		req := protocol.NewSystemMessage(protocol.OpMoveFragment)
		req.SetValue([]byte("fragment-data"))
		req.SetBuffer(bytes.NewBuffer(nil))
		_, err = s.extractFragmentPack(req)
		if err == nil {
			t.Fatalf("Expected an error. Got: %v", err)
		}
	})

	t.Run("selectVersionForMerge", func(t *testing.T) {
		currentValue := []byte("current-value")
		e := dm.engine.NewEntry()
		e.SetKey(key)
		e.SetTimestamp(time.Now().UnixNano())
		e.SetValue(currentValue)
		winner, err := dm.selectVersionForMerge(f, hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(winner.Value(), currentValue) {
			t.Fatalf("Winner is different")
		}
	})
}
