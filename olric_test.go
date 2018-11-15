// Copyright 2018 Burak Sezer
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
	"context"
	"io/ioutil"
	"os"
	"testing"
)

func Test_ReloadSnapshot(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "olric-snapshot")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	defer func() {
		err = os.RemoveAll(dir)
		if err != nil {
			t.Logf("[ERROR] Failed to remove data dir: %s: %v", dir, err)
		}
	}()

	db, err := newOlricWithSnapshot(nil, dir)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm := db.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// Shutdown this instance. We will try to restore DMaps from disk.
	err = db.Shutdown(context.Background())
	if err != nil {
		db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
	}

	db2, err := newOlricWithSnapshot(nil, dir)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		value, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value reloaded from Badger for %d", i)
		}
	}
}
