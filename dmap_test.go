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

package olricdb

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
)

func bkey(i int) string {
	return fmt.Sprintf("%09d", i)
}

func bval(i int) []byte {
	return []byte(fmt.Sprintf("%025d", i))
}

func newTestOlricDB(peers []string, mc *memberlist.Config) (*OlricDB, *httptest.Server, error) {
	srv := httptest.NewUnstartedServer(nil)
	mc.BindPort = 0
	mc.Name = srv.Listener.Addr().String()
	cfg := &Config{
		PartitionCount:   7,
		BackupCount:      1,
		Name:             mc.Name,
		Peers:            peers,
		MemberlistConfig: mc,
	}
	r, err := New(cfg)
	if err != nil {
		return nil, nil, err
	}

	err = r.prepare()
	if err != nil {
		return nil, nil, err
	}
	srv.Config = &http.Server{Handler: r.transport.router}
	srv.Start()

	err = r.transport.checkAliveness(srv.Listener.Addr().String())
	if err != nil {
		return nil, nil, err
	}
	return r, srv, nil
}

func newOlricDBWithCustomMemberlist(peers []string) (*OlricDB, *httptest.Server, error) {
	mc := memberlist.DefaultLocalConfig()
	mc.IndirectChecks = 0
	mc.SuspicionMult = 1
	mc.TCPTimeout = 50 * time.Millisecond
	mc.ProbeInterval = 10 * time.Millisecond
	return newTestOlricDB(peers, mc)
}

func newOlricDB(peers []string) (*OlricDB, *httptest.Server, error) {
	mc := memberlist.DefaultLocalConfig()
	return newTestOlricDB(peers, mc)
}

func TestDMap_Standalone(t *testing.T) {
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	key := "mykey"
	value := "myvalue"
	// Create a new DMap object and put a K/V pair.
	d := r.NewDMap("foobar")
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
	r, srv, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv.Close()
	defer func() {
		err = r.Shutdown(context.Background())
		if err != nil {
			r.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), nil)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm2 := r2.NewDMap("mymap")
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	for i := 0; i < 100; i++ {
		member, hkey, err := r1.locateKey("mymap", bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		var owner *OlricDB
		if hostCmp(member, r1.this) {
			owner = r1
		} else {
			owner = r2
		}
		value, err := owner.getKeyVal(hkey, "mymap")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if !bytes.Equal(value.([]byte), bval(i)) {
			t.Fatalf("Different value retrieved for %s from %s", bkey(i), owner.this)
		}
	}
}

func TestDMap_PutLookup(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Dont move partitions during test.
	r1.fsckMtx.Lock()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Write again
	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err := dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// We should see modified values on r1
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
	r1.fsckMtx.Unlock()
	r1.fsck()
}

func TestDMap_Get(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// Dont move partitions. We check and retrieve keys from the previous owner.
	r1.fsckMtx.Lock()
	defer r1.fsckMtx.Unlock()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// Don't move partitions to r2
	r1.fsckMtx.Lock()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	dm2 := r2.NewDMap("mymap")
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

	// To prevent useless error messages
	r1.fsckMtx.Unlock()
	r1.fsck()
}

func TestDMap_PruneHosts(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	peers = []string{r1.discovery.localNode().Address(), r2.discovery.localNode().Address()}
	r3, srv3, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv3.Close()
	defer func() {
		err = r3.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	instances := []*OlricDB{r1, r2, r3}
	for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
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

func TestDMap_DestroyDmap(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()

	dm := r1.NewDMap("mymap")
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
	r1, srv1, err := newOlricDBWithCustomMemberlist(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDBWithCustomMemberlist(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers = append(peers, r2.discovery.localNode().Address())
	r3, srv3, err := newOlricDBWithCustomMemberlist(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv3.Close()
	defer func() {
		err = r3.Shutdown(context.Background())
		if err != nil {
			r3.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	events := r2.discovery.subscribeNodeEvents()
	srv1.Close()
	err = r1.Shutdown(context.Background())
	if err != nil {
		t.Fatalf("Failed to shutdown OlricDB: %v", err)
	}
	<-events
	// Remove from consistent hash table, otherwise partition distributing algorithm is broken
	r2.consistent.Remove(r1.this.String())
	r3.consistent.Remove(r1.this.String())
	r2.updateRouting()

	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		_, err = dm2.Get(bkey(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v for %s: %s", err, bkey(i), r1.this)
		}
	}
}

func TestDMap_PutEx(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.PutEx(bkey(i), bval(i), 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 100; i++ {
		_, err := dm.Get(bkey(i))
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound. Got: %v", err)
		}
	}
}

func TestDMap_TTLEviction(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	r1.updateRouting()

	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.PutEx(bkey(i), bval(i), 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 100; i++ {
		r1.evictKeys()
		r2.evictKeys()
	}
	length := 0
	for _, ins := range []*OlricDB{r1, r2} {
		for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
			part := ins.partitions[partID]
			part.RLock()
			for _, dm := range part.m {
				dm.RLock()
				length += len(dm.d)
				dm.RUnlock()
			}
			part.RUnlock()
		}
	}
	if length == 100 {
		t.Fatalf("Expected key count is different than 100")
	}
}

func TestDMap_DeleteStaleDMaps(t *testing.T) {
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	r1.updateRouting()
	r1.fsck()

	mname := "mymap"
	dm := r1.NewDMap(mname)
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

	var dc int
	for i := 0; i < 1000; i++ {
		dc = 0
		for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
			for _, instance := range []*OlricDB{r1, r2} {
				part := instance.partitions[partID]
				part.Lock()
				dc += len(part.m)
				part.Unlock()

				bpart := instance.backups[partID]
				bpart.Lock()
				dc += len(bpart.m)
				bpart.Unlock()
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
	r1, srv1, err := newOlricDB(nil)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv1.Close()
	defer func() {
		err = r1.Shutdown(context.Background())
		if err != nil {
			r1.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()
	fmt.Println(r1.discovery.memberlist.LocalNode().Name)
	dm := r1.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err = dm.Put(bkey(i), bval(i))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}

	// Dont move partitions during test.
	r1.fsckMtx.Lock()
	defer r1.fsckMtx.Unlock()

	peers := []string{r1.discovery.localNode().Address()}
	r2, srv2, err := newOlricDB(peers)
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer srv2.Close()
	defer func() {
		err = r2.Shutdown(context.Background())
		if err != nil {
			r2.logger.Printf("[ERROR] Failed to shutdown OlricDB: %v", err)
		}
	}()

	// Write again
	dm2 := r2.NewDMap("mymap")
	for i := 0; i < 100; i++ {
		err := dm2.Put(bkey(i), []byte(bkey(i)+"-v2"))
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}
	// The outdated values on secondary owners should be deleted.
	for _, ins := range []*OlricDB{r1, r2} {
		for partID := uint64(0); partID < r1.config.PartitionCount; partID++ {
			part := ins.partitions[partID]
			part.RLock()
			// We should see modified values on r1
			for i := 0; i < 100; i++ {
				dmp := part.m["mymap"]
				if dmp == nil {
					continue
				}
				dmp.RLock()
				key := bkey(i)
				hkey := r1.getHKey("mymap", key)
				value, ok := dmp.d[hkey]
				if ok {
					val := []byte(bkey(i) + "-v2")
					if !bytes.Equal(value.Value.([]byte), val) {
						t.Fatalf("Different value retrieved for %s", key)
					}
				}
				dmp.RUnlock()
			}
			part.RUnlock()
		}
	}
}
