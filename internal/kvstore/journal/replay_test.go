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
	"bytes"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/testutil"
	"github.com/cespare/xxhash"
)

func TestJournal_Replay(t *testing.T) {
	f, err := testutil.CreateTmpfile(t, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	var replayed bool
	c := &Config{
		Path: f.Name(),
		ReplayDone: func() {
			replayed = true
		},
	}
	j, err := New(c)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = j.Close()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
	err = j.Start()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		e := NewMockEntry()
		e.SetKey(testutil.ToKey(i))
		e.SetValue(testutil.ToVal(i))
		e.SetTimestamp(time.Now().UnixNano())
		e.SetTTL(18071988)
		hkey := xxhash.Sum64String(testutil.ToKey(i))

		err = j.Append(OpPut, hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = j.Append(OpUpdateTTL, hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = j.Append(OpDelete, hkey, e)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	<-time.After(250 * time.Millisecond)

	var i, total int
	err = j.Replay(f.Name(), func(opcode OpCode, hkey uint64, value []byte) {
		e := NewMockEntry()
		e.Decode(value)
		key := testutil.ToKey(total)
		if e.Key() != key {
			t.Fatalf("Expected key: %s. Got: %s", key, e.Key())
		}
		if !bytes.Equal(e.Value(), testutil.ToVal(total)) {
			t.Fatalf("Value is differrent")
		}
		if xxhash.Sum64String(key) != xxhash.Sum64String(e.Key()) {
			t.Fatalf("HKey is different")
		}

		i++
		switch i {
		case 1:
			if opcode != OpPut {
				t.Fatalf("Expected opcode: %d. Got: %d", opcode, OpPut)
			}
		case 2:
			if opcode != OpUpdateTTL {
				t.Fatalf("Expected opcode: %d. Got: %d", opcode, OpUpdateTTL)
			}
		case 3:
			if opcode != OpDelete {
				t.Fatalf("Expected opcode: %d. Got: %d", opcode, OpDelete)
			}
		}

		if i == 3 {
			total++
			i = 0
		}
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if !replayed {
		t.Fatalf("ReplayDone hook doesnt work.")
	}
}
