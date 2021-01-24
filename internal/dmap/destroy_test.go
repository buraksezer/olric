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
	"testing"

	"github.com/buraksezer/olric/internal/testcluster"
	"github.com/buraksezer/olric/internal/testutil"
)

func Test_Destroy_DMapOnCluster(t *testing.T) {
	cluster := testcluster.New(NewService)
	s1 := cluster.AddMember(nil).(*Service)
	cluster.AddMember(nil)
	defer cluster.Shutdown()

	dm, err := s1.NewDMap("mymap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	for i := 0; i < 100; i++ {
		err = dm.Put(testutil.ToKey(i), testutil.ToVal(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = dm.Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Get(testutil.ToKey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}
