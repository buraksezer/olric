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

package snapshot

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
)

const testPartitionCount uint64 = 7

func newSnapshot() (string, *Snapshot, error) {
	dir, err := ioutil.TempDir("/tmp", "olric-snapshot")
	if err != nil {
		return "", nil, err
	}
	opt := badger.DefaultOptions
	opt.Dir = dir
	opt.ValueDir = dir
	logger := log.New(os.Stderr, "", log.LstdFlags)
	s, err := New(&opt, defaultSnapshotInterval, 0, 0, logger)
	return dir, s, err
}

func Test_Put(t *testing.T) {
	tmpdir, snap, err := newSnapshot()
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = snap.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	oplogs := make(map[uint64]*OpLog)
	for partID := uint64(0); partID < testPartitionCount; partID++ {
		oh, err := offheap.New(0)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplog, err := snap.RegisterDMap(PrimaryDMapKey, partID, strconv.Itoa(int(partID)), oh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplogs[partID] = oplog
	}
	for hkey := uint64(0); hkey < uint64(100); hkey++ {
		for _, oplog := range oplogs {
			vdata := &offheap.VData{
				Key:   strconv.Itoa(int(hkey)),
				TTL:   1,
				Value: []byte("value"),
			}
			// Store data on Olric's off-heap store.
			err = oplog.off.Put(hkey, vdata)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			// Call Put on operation log.
			oplog.Put(hkey)
			break
		}
	}

	// Syncs to the disk 10 times per second, by default.
	<-time.After(150 * time.Millisecond)
	err = snap.db.View(func(txn *badger.Txn) error {
		// Now check the hkeys on the disk.
		for hkey := uint64(0); hkey < uint64(100); hkey++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, hkey)
			_, err = txn.Get(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func Test_Delete(t *testing.T) {
	tmpdir, snap, err := newSnapshot()
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = snap.Shutdown()
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
			return
		}
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	oplogs := make(map[uint64]*OpLog)
	for partID := uint64(0); partID < testPartitionCount; partID++ {
		oh, err := offheap.New(0)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplog, err := snap.RegisterDMap(PrimaryDMapKey, partID, strconv.Itoa(int(partID)), oh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplogs[partID] = oplog
	}

	partitions := make(map[uint64][]uint64)
	for hkey := uint64(0); hkey < uint64(100); hkey++ {
		for partID, oplog := range oplogs {
			vdata := &offheap.VData{
				Key:   strconv.Itoa(int(hkey)),
				TTL:   1,
				Value: []byte("value"),
			}
			// Store data on Olric's off-heap store.
			err = oplog.off.Put(hkey, vdata)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			// Call Put on operation log.
			oplog.Put(hkey)
			partitions[partID] = append(partitions[partID], hkey)
			break
		}
	}

	// Syncs to the disk 10 times per second, by default.
	<-time.After(150 * time.Millisecond)

	// Delete the keys from snapshot.
	for partID, hkeys := range partitions {
		oplog := oplogs[partID]
		for _, hkey := range hkeys {
			oplog.Delete(hkey)
		}
	}

	// Wait again to remove from badger.
	<-time.After(150 * time.Millisecond)

	err = snap.db.View(func(txn *badger.Txn) error {
		// Now check the hkeys on the disk.
		for hkey := uint64(0); hkey < uint64(100); hkey++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, hkey)
			_, err = txn.Get(k)
			if err == badger.ErrKeyNotFound {
				continue
			}
			return fmt.Errorf("invalid response from BadgerDB for %d: %v", hkey, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}

func Test_Loader(t *testing.T) {
	tmpdir, snap, err := newSnapshot()
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = snap.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	oplogs := make(map[uint64]*OpLog)
	for partID := uint64(0); partID < testPartitionCount; partID++ {
		oh, err := offheap.New(0)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplog, err := snap.RegisterDMap(PrimaryDMapKey, partID, "test", oh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplogs[partID] = oplog
	}
	for hkey := uint64(0); hkey < uint64(100); hkey++ {
		for _, oplog := range oplogs {
			vdata := &offheap.VData{
				Key:   strconv.Itoa(int(hkey)),
				TTL:   1,
				Value: []byte("value"),
			}
			// Store data on Olric's off-heap store.
			err = oplog.off.Put(hkey, vdata)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			// Call Put on operation log.
			oplog.Put(hkey)
			break
		}
	}

	// Syncs to the disk 10 times per second, by default.
	<-time.After(150 * time.Millisecond)

	l, err := snap.NewLoader(PrimaryDMapKey)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	parts := uint64(0)
	offs := []*offheap.Offheap{}
	for {
		dm, err := l.Next()
		if err == ErrLoaderDone {
			break
		}
		parts++
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if dm.Name != "test" {
			t.Fatalf("Expected dmap name: test. Got: %s", dm.Name)
		}
		offs = append(offs, dm.Off)
	}
	if parts != testPartitionCount {
		t.Fatalf("Expected partition count %d. Got: %d", testPartitionCount, parts)
	}

	for hkey := uint64(0); hkey < uint64(100); hkey++ {
		var found bool
		for _, off := range offs {
			_, err := off.Get(hkey)
			if err == offheap.ErrKeyNotFound {
				continue
			}
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			found = true
			break
		}
		if !found {
			t.Fatalf("HKey: %d could not be found in snapshot", hkey)
		}
	}
}

func Test_DestroyDMap(t *testing.T) {
	tmpdir, snap, err := newSnapshot()
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = snap.Shutdown()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = os.RemoveAll(tmpdir)
		if err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()
	oplogs := make(map[uint64]*OpLog)
	for partID := uint64(0); partID < testPartitionCount; partID++ {
		oh, err := offheap.New(0)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplog, err := snap.RegisterDMap(PrimaryDMapKey, partID, "test", oh)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		oplogs[partID] = oplog
	}
	for hkey := uint64(0); hkey < uint64(100); hkey++ {
		for _, oplog := range oplogs {
			vdata := &offheap.VData{
				Key:   strconv.Itoa(int(hkey)),
				TTL:   1,
				Value: []byte("value"),
			}
			// Store data on Olric's off-heap store.
			err = oplog.off.Put(hkey, vdata)
			if err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
			// Call Put on operation log.
			oplog.Put(hkey)
			break
		}
	}

	// Syncs to the disk 10 times per second, by default.
	<-time.After(150 * time.Millisecond)
	for partID := uint64(0); partID < testPartitionCount; partID++ {
		err = snap.DestroyDMap(PrimaryDMapKey, partID, "test")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	err = snap.db.View(func(txn *badger.Txn) error {
		// Now check the hkeys on the disk.
		for hkey := uint64(0); hkey < uint64(100); hkey++ {
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, hkey)
			_, err = txn.Get(k)
			if err == badger.ErrKeyNotFound {
				continue
			}
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
}
