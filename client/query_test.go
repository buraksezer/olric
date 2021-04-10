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

package client

import (
	"strconv"
	"strings"
	"testing"

	"github.com/buraksezer/olric/internal/testolric"
	"github.com/buraksezer/olric/query"
)

func TestClient_Query(t *testing.T) {
	srv, err := testolric.New(t)
	if err != nil {
		t.Fatalf("Expected nil. Got %v", err)
	}
	tc := newTestConfig(srv)

	c, err := New(tc)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	dm := c.NewDMap("mymap")
	var key string
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			key = "even:" + strconv.Itoa(i)
		} else {
			key = "odd:" + strconv.Itoa(i)
		}
		err = dm.Put(key, i)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	q, err := dm.Query(query.M{"$onKey": query.M{"$regexMatch": "even:"}})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = q.Range(func(key string, value interface{}) bool {
		if !strings.HasPrefix(key, "even:") {
			t.Fatalf("Expected prefix: even:. Got: %s", key)
		}
		return true
	})

	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
