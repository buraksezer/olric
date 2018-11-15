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
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/buraksezer/olric/internal/snapshot"
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/memberlist"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func getRandomAddr() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.Addr().String(), nil
}

func newTestOlric(peers []string, mc *memberlist.Config, snapshotDir string) (*Olric, error) {
	addr, err := getRandomAddr()
	if err != nil {
		return nil, err
	}
	if mc == nil {
		mc = memberlist.DefaultLocalConfig()
	}
	mc.Name = addr
	mc.BindPort = 0
	cfg := &Config{
		PartitionCount:   7,
		BackupCount:      1,
		Name:             mc.Name,
		Peers:            peers,
		MemberlistConfig: mc,
	}
	if len(snapshotDir) != 0 {
		opt := badger.DefaultOptions
		opt.Dir = snapshotDir
		opt.ValueDir = snapshotDir
		cfg.BadgerOptions = &opt
		cfg.OperationMode = OpInMemoryWithSnapshot
	}
	db, err := New(cfg)
	if err != nil {
		return nil, err
	}

	if cfg.OperationMode == OpInMemoryWithSnapshot {
		err := db.restoreFromSnapshot(snapshot.PrimaryDMapKey)
		if err != nil {
			return nil, err
		}
		err = db.restoreFromSnapshot(snapshot.BackupDMapKey)
		if err != nil {
			return nil, err
		}
	}

	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		err = db.server.ListenAndServe()
		if err != nil {
			db.log.Printf("[ERROR] Failed to run TCP server")
		}
	}()
	<-db.server.StartCh

	err = db.startDiscovery()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newOlricWithCustomMemberlist(peers []string) (*Olric, error) {
	mc := memberlist.DefaultLocalConfig()
	mc.IndirectChecks = 0
	mc.SuspicionMult = 1
	mc.TCPTimeout = 50 * time.Millisecond
	mc.ProbeInterval = 10 * time.Millisecond
	return newTestOlric(peers, mc, "")
}

func newOlric(peers []string) (*Olric, error) {
	return newTestOlric(peers, nil, "")
}

func newOlricWithSnapshot(peers []string, snapshotDir string) (*Olric, error) {
	return newTestOlric(peers, nil, snapshotDir)
}

func TestDMap_Standalone(t *testing.T) {
	db, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	key := "mykey"
	value := "myvalue"
	// Create a new DMap object and put a K/V pair.
	d := db.NewDMap("foobar")
	err = d.Put(key, value)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Get the value and check it.
	val, err := d.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != value {
		t.Fatalf("Expected value %v. Got: %v", value, val)
	}

	// Delete it and check again.
	err = d.Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = d.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_NilValue(t *testing.T) {
	r, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	key := "mykey"
	// Create a new DMap object and put a K/V pair.
	d := r.NewDMap("foobar")
	err = d.Put(key, nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	// Get the value and check it.
	val, err := d.Get(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	if val != nil {
		t.Fatalf("Expected value nil. Got: %v", val)
	}

	// Delete it and check again.
	err = d.Delete(key)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = d.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
	}
}

func TestDMap_NilValueWithTwoMembers(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		// Get the value and check it.
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if val != nil {
			t.Fatalf("Expected value nil. Got: %v", val)
		}
	}
}

func TestDMap_Put(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		val, err := dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(val.([]byte), bval(i)) {
			t.Errorf("Different value(%s) retrieved for %s", val.([]byte), bkey(i))
		}
	}
}

func TestDMap_PutLookup(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Dont move partitions during test.
	db1.fsckMx.Lock()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	// Write again
	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err := dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// We should see modified values on db1
	for i := 0; i < 100; i++ {
		value, err := dm.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		val := []byte(bkey(i) + "-v2")
		if !bytes.Equal(value.([]byte), val) {
			t.Fatalf("Different value retrieved for %s", bkey(i))
		}
	}

	// To prevent useless error messages
	db1.fsckMx.Unlock()
	db1.fsck()
}

func TestDMap_Get(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		value, err := dm.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value retrieved for %s", bkey(i))
		}
	}
}

func TestDMap_Delete(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm.Delete(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		// TODO: BDD
		time.Sleep(10 * time.Millisecond)
		_, err := dm.Get(key)
		if err == nil {
			t.Fatalf("Expected an error. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_GetLookup(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// Dont move partitions. We check and retrieve keys from the previous owner.
	db1.fsckMx.Lock()
	defer db1.fsckMx.Unlock()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	for i := 0; i < 100; i++ {
		key := bkey(i)
		value, err := dm.Get(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value retrieved for %s", bkey(i))
		}
	}
}

func TestDMap_DeleteLookup(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// Don't move partitions to db2
	db1.fsckMx.Lock()
	// To prevent useless error messages
	defer func() {
		db1.fsckMx.Unlock()
		db1.fsck()
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		key := bkey(i)
		err = dm2.Delete(key)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s", err, key)
		}
	}

	for i := 0; i < 100; i++ {
		key := bkey(i)
		_, err = dm2.Get(key)
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v for %s", err, key)
		}
	}
}

func TestDMap_PruneHosts(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers = []string{db1.discovery.localNode().Address(), db2.discovery.localNode().Address()}
	r3, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r3.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	instances := []*Olric{db1, db2, r3}
	for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
		for _, db := range instances {
			part := db.partitions[partID]
			part.Lock()
			if len(part.owners) != 1 {
				t.Fatalf("Expected owner count is 1. Got: %d", len(part.owners))
			}
			part.Unlock()
		}
	}
}

func TestDMap_Destroy(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	err = dm.Destroy()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err = dm.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_CrashServer(t *testing.T) {
	db1, err := newOlricWithCustomMemberlist(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlricWithCustomMemberlist(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers = append(peers, db2.discovery.localNode().Address())
	r3, err := newOlricWithCustomMemberlist(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = r3.Shutdown(context.Background())
		if err != nil {
			r3.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	events := db2.discovery.subscribeNodeEvents()
	err = db1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Failed to shutdown Olric: %v", err)
	}
	<-events
	// Remove from consistent hash table, otherwise partition distributing algorithm is broken
	db2.consistent.Remove(db1.this.String())
	r3.consistent.Remove(db1.this.String())
	db2.updateRouting()

	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		_, err = dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s: %s", err, bkey(i), db1.this)
		}
	}
}

func TestDMap_PutEx(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.PutEx(bkey(i), bval(i), 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Update currentUnixNano to evict the key now.
	atomic.StoreInt64(&currentUnixNano, time.Now().UnixNano())
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 100; i++ {
		_, err := dm.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_TTLEviction(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	db1.updateRouting()

	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.PutEx(bkey(i), bval(i), 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	// Update currentUnixNano to evict the key now.
	atomic.StoreInt64(&currentUnixNano, time.Now().UnixNano())
	for i := 0; i < 100; i++ {
		db1.evictKeys()
		db2.evictKeys()
	}
	length := 0
	for _, ins := range []*Olric{db1, db2} {
		for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
			part := ins.partitions[partID]
			part.m.Range(func(k, v interface{}) bool {
				dm := v.(*dmap)
				length += dm.off.Len()
				return true
			})
		}
	}
	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_DeleteStaleDMaps(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	db1.updateRouting()
	db1.fsck()

	mname := "mymap"
	dm := db1.NewDMap(mname)
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		err = dm.Delete(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	db1.deleteStaleDMaps()
	db2.deleteStaleDMaps()

	var dc int32
	for i := 0; i < 1000; i++ {
		dc = 0
		for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
			for _, instance := range []*Olric{db1, db2} {
				part := instance.partitions[partID]
				dc += atomic.LoadInt32(&part.count)
				bpart := instance.backups[partID]
				dc += atomic.LoadInt32(&bpart.count)
			}
		}
		if dc == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if dc != 0 {
		t.Fatalf("Expected dmap count is 0. Got: %d", dc)
	}
}

func TestDMap_PutPurgeOldVersions(t *testing.T) {
	db1, err := newOlric(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db1.Shutdown(context.Background())
		if err != nil {
			db1.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()
	dm := db1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Dont move partitions during test.
	db1.fsckMx.Lock()
	defer db1.fsckMx.Unlock()

	peers := []string{db1.discovery.localNode().Address()}
	db2, err := newOlric(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db2.Shutdown(context.Background())
		if err != nil {
			db2.log.Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	// Write again
	dm2 := db2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// The outdated values on secondary owners should be deleted.
	for _, ins := range []*Olric{db1, db2} {
		for partID := uint64(0); partID < db1.config.PartitionCount; partID++ {
			part := ins.partitions[partID]
			part.RLock()
			// We should see modified values on db1
			for i := 0; i < 100; i++ {
				tmp, ok := part.m.Load("mymap")
				if !ok {
					continue
				}
				dm := tmp.(*dmap)
				key := bkey(i)
				hkey := db1.getHKey("mymap", key)
				value, err := dm.off.Get(hkey)
				// Some keys are owned by the second node.
				if err == offheap.ErrKeyNotFound {
					continue
				}
				if err != nil {
					t.Fatalf("Expected nil. Got: %v", err)
				}
				var val interface{}
				err = db1.serializer.Unmarshal(value.Value, &val)
				if err != nil {
					t.Fatalf("Expected nil. Got: %v", err)
				}
				if !bytes.Equal(val.([]byte), []byte(bkey(i)+"-v2")) {
					t.Fatalf("Different value retrieved for %s", key)
				}
			}
			part.RUnlock()
		}
	}
}
