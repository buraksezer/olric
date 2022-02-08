// Copyright 2018-2022 Burak Sezer
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
	"errors"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func TestDMap_Stats(t *testing.T) {
	cluster := testcluster.New(NewService)
	s := cluster.AddMember(nil).(*Service)
	defer cluster.Shutdown()

	dm, err := s.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	// EntriesTotal
	for i := 0; i < 10; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	//GetHits
	for i := 0; i < 10; i++ {
		_, err = dm.Get(testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// DeleteHits
	for i := 0; i < 10; i++ {
		err = dm.Delete(testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// GetMisses
	for i := 0; i < 10; i++ {
		_, err = dm.Get(testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	// DeleteMisses
	for i := 0; i < 10; i++ {
		err = dm.Delete(testutil.ToKey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// EntriesTotal, EvictedTotal
	for i := 0; i < 10; i++ {
		err = dm.PutEx(testutil.ToKey(i), testutil.ToVal(i), time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	<-time.After(100 * time.Millisecond)

	// GetMisses
	for i := 0; i < 10; i++ {
		_, err = dm.Get(testutil.ToKey(i))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}

	stats := map[string]int64{
		"EntriesTotal": EntriesTotal.Read(),
		"GetMisses":    GetMisses.Read(),
		"GetHits":      GetHits.Read(),
		"DeleteHits":   DeleteHits.Read(),
		"DeleteMisses": DeleteMisses.Read(),
		"EvictedTotal": EvictedTotal.Read(),
	}
	for name, value := range stats {
		if value <= 0 {
			t.Fatalf("Expected %s has to be bigger than zero", name)
		}
	}
}
