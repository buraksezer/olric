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
	"encoding/gob"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"
)

const nilTimeout = 0 * time.Second

var (
	// ErrKeyNotFound is returned when a key could not be found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrOperationTimeout is returned when an operation times out.
	ErrOperationTimeout = errors.New("operation timeout")
)

var bootstrapTimeoutDuration = 10 * time.Second

// DMap represents a distributed map object.
type DMap struct {
	name string
	db   *OlricDB
}

// NewDMap creates an returns a new DMap object.
func (db *OlricDB) NewDMap(name string) *DMap {
	return &DMap{
		name: name,
		db:   db,
	}
}

func registerValueType(value interface{}) {
	t := reflect.TypeOf(value)
	v := reflect.New(t).Elem().Interface()
	gob.Register(v)
}

func (db *OlricDB) purgeOldVersions(hkey uint64, name string) {
	owners := db.getPartitionOwners(hkey)
	// Remove the key/value pair on the previous owners
	owners = owners[:len(owners)-1]
	for i := 1; i <= len(owners); i++ {
		// Traverse in reverse order.
		idx := len(owners) - i
		owner := owners[idx]
		if hostCmp(owner, db.this) {
			// If the partition's primary owner has been changed by the coordinator node
			// don't try to remove the on itself.
			continue
		}
		if err := db.transport.deletePrev(owner, hkey, name); err != nil {
			db.logger.Printf("[ERROR] Failed to remove purge %s:%d on %s", name, hkey, owner)
		}
	}
}

func (db *OlricDB) putKeyVal(hkey uint64, name string, value interface{}, timeout time.Duration) error {
	dmp := db.getDMap(name, hkey)
	dmp.Lock()
	defer dmp.Unlock()

	var ttl int64
	if timeout != nilTimeout {
		ttl = getTTL(timeout)
	}
	val := vdata{
		Value: value,
		TTL:   ttl,
	}

	if db.config.BackupCount != 0 {
		if db.config.BackupMode == AsyncBackupMode {
			db.wg.Add(1)
			go func() {
				defer db.wg.Done()
				err := db.putKeyValBackup(hkey, name, value, timeout)
				if err != nil {
					db.logger.Printf("[ERROR] Failed to create backup mode in async mode: %v", err)
				}
			}()
		} else {
			err := db.putKeyValBackup(hkey, name, value, timeout)
			if err != nil {
				return fmt.Errorf("failed to create backup in sync mode: %v", err)
			}
		}
	}
	dmp.d[hkey] = val
	db.purgeOldVersions(hkey, name)
	return nil
}

func (dm *DMap) put(key string, value interface{}, timeout time.Duration) error {
	member, hkey, err := dm.db.locateKey(dm.name, key)
	if err != nil {
		return err
	}

	if value == nil {
		value = struct{}{}
	}

	registerValueType(value)
	if !hostCmp(member, dm.db.this) {
		return dm.db.transport.put(member, hkey, dm.name, value, timeout)
	}
	return dm.db.putKeyVal(hkey, dm.name, value, timeout)
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// The key has to be string. Value type is arbitrary. It is safe to modify the contents of the arguments after Put returns but not before.
func (dm *DMap) Put(key string, value interface{}) error {
	return dm.put(key, value, nilTimeout)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// The key has to be string. Value type is arbitrary. It is safe to modify the contents of the arguments after Put returns but not before.
func (dm *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	return dm.put(key, value, timeout)
}

func (db *OlricDB) getKeyVal(hkey uint64, name string) (interface{}, error) {
	dmp := db.getDMap(name, hkey)
	dmp.RLock()
	defer dmp.RUnlock()
	value, ok := dmp.d[hkey]
	if ok {
		if isKeyExpired(value.TTL) {
			return nil, ErrKeyNotFound
		}
		if _, ok := value.Value.(struct{}); ok {
			return nil, nil
		}
		return value.Value, nil
	}

	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}
	owners = owners[:len(owners)-1]
	for i := 1; i <= len(owners); i++ {
		// Traverse in reverse order.
		idx := len(owners) - i
		owner := owners[idx]
		target := url.URL{
			Scheme: db.transport.scheme,
			Host:   owner.String(),
			Path:   path.Join("/get-prev", name, printHKey(hkey)),
		}
		rawval, err := db.transport.doRequest(http.MethodGet, target, nil)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		buf := bytes.NewReader(rawval)
		var value interface{}
		err = gob.NewDecoder(buf).Decode(&value)
		if err != nil {
			return nil, err
		}
		if _, ok := value.(struct{}); ok {
			return nil, nil
		}
		return value, nil
	}

	backups := db.getBackupPartitionOwners(hkey)
	for _, backup := range backups {
		target := url.URL{
			Scheme: db.transport.scheme,
			Host:   backup.String(),
			Path:   path.Join("/backup/get", name, printHKey(hkey)),
		}
		rawval, err := db.transport.doRequest(http.MethodGet, target, nil)
		if err == ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		buf := bytes.NewReader(rawval)
		var value interface{}
		err = gob.NewDecoder(buf).Decode(&value)
		if err != nil {
			return nil, err
		}
		if _, ok := value.(struct{}); ok {
			return nil, nil
		}
		return value, nil
	}

	// It's not there, really.
	return nil, ErrKeyNotFound
}

// Get gets the value for the given key. It returns ErrKeyNotFound if the DB does not contains the key. It's thread-safe.
// It is safe to modify the contents of the returned value. It is safe to modify the contents of the argument after Get returns.
func (dm *DMap) Get(key string) (interface{}, error) {
	member, hkey, err := dm.db.locateKey(dm.name, key)
	if err != nil {
		return nil, err
	}
	if !hostCmp(member, dm.db.this) {
		return dm.db.transport.get(member, hkey, dm.name)
	}
	return dm.db.getKeyVal(hkey, dm.name)
}

func (db *OlricDB) deleteStaleDMap(name string, hkey uint64, backup bool) {
	defer db.wg.Done()

	var part *partition
	if backup {
		part = db.getBackupPartition(hkey)
	} else {
		part = db.getPartition(hkey)
	}

	part.Lock()
	defer part.Unlock()
	dm, ok := part.m[name]
	if !ok {
		return
	}

	dm.Lock()
	defer dm.Unlock()
	if len(dm.d) == 0 {
		delete(part.m, name)
	}
}

func (db *OlricDB) deleteKeyVal(hkey uint64, name string) error {
	dmp := db.getDMap(name, hkey)
	dmp.Lock()
	defer dmp.Unlock()
	return db.delKeyVal(dmp, hkey, name)
}

func (db *OlricDB) delKeyVal(dmp *dmap, hkey uint64, name string) error {
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	// Remove the key/value pair on the other owners
	owners = owners[:len(owners)-1]
	for i := 1; i <= len(owners); i++ {
		// Traverse in reverse order.
		idx := len(owners) - i
		owner := owners[idx]
		if err := db.transport.deletePrev(owner, hkey, name); err != nil {
			return err
		}

	}
	if db.config.BackupCount != 0 {
		err := db.deleteKeyValBackup(hkey, name)
		if err != nil {
			return err
		}
	}
	delete(dmp.d, hkey)
	if len(dmp.d) == 0 {
		db.wg.Add(1)
		go db.deleteStaleDMap(name, hkey, false)
	}
	return nil
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(key string) error {
	member, hkey, err := dm.db.locateKey(dm.name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, dm.db.this) {
		return dm.db.transport.delete(member, hkey, dm.name)
	}
	return dm.db.deleteKeyVal(hkey, dm.name)
}

// Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps. So if you call Put/PutEx and Destroy
// methods concurrently on the cluster, Put/PutEx calls may set new values to the DMap.
func (dm *DMap) Destroy() error {
	<-dm.db.bctx.Done()
	if dm.db.bctx.Err() == context.DeadlineExceeded {
		return ErrOperationTimeout
	}

	var g errgroup.Group
	for _, item := range dm.db.discovery.getMembers() {
		addr := item.String()
		g.Go(func() error {
			err := dm.db.transport.destroyDmap(dm.name, addr)
			if err != nil {
				dm.db.logger.Printf("Failed to destroy dmap:%s on %s", dm.name, addr)
			}
			return err
		})
	}
	return g.Wait()
}

func getTTL(timeout time.Duration) int64 {
	return (timeout.Nanoseconds() + time.Now().UnixNano()) / 1000000
}

func isKeyExpired(ttl int64) bool {
	if ttl == 0 {
		return false
	}
	return (time.Now().UnixNano() / 1000000) >= ttl
}
