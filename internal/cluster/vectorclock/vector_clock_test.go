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

package vectorclock

import (
	"reflect"
	"sync"
	"testing"
)

func TestVectorClock_Set_Get(t *testing.T) {
	v := New()

	v.Set(1, 10)

	timestamp, ok := v.Get(1)

	if !ok {
		t.Fatalf("Expected true. Got false")
	}

	if timestamp != 10 {
		t.Fatalf("Expected timestamp is 10. Got: %v", timestamp)
	}
}

func TestVectorClock_Tick(t *testing.T) {
	v := New()

	v.Set(1, 10)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.Tick(1)
		}()
	}

	wg.Wait()

	timestamp, ok := v.Get(1)

	if !ok {
		t.Fatalf("Expected true. Got false")
	}

	if timestamp != 110 {
		t.Fatalf("Expected timestamp is 110. Got: %v", timestamp)
	}
}

func TestVectorClock_Encode_Decode(t *testing.T) {
	v := New()

	v.Set(1, 1)
	v.Set(2, 1)
	v.Set(3, 1)

	data := v.Encode()
	r := New()
	r.Decode(data)
	if !reflect.DeepEqual(v, r) {
		t.Fatalf("Deocded VectorClock is different")
	}
}
