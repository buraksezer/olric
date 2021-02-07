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

package journal

import (
	"fmt"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/cespare/xxhash"
)

func TestJournal_Append(t *testing.T) {
	c := &Config{
		Path: "/Users/exp/deneme",
	}
	j, err := New(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	err = j.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = j.Close()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()

	for i := 0; i < 1000; i++ {
		e := NewMockEntry()
		e.SetKey(testutil.ToKey(i))
		e.SetValue(testutil.ToVal(i))
		e.SetTimestamp(time.Now().UnixNano())
		e.SetTTL(18071988)
		hkey := xxhash.Sum64String(testutil.ToKey(i))

		j.Append(OpPUT, hkey, e)
	}

	<-time.After(time.Second)

	fmt.Println(j.Stats())
}
