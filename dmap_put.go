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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/buraksezer/olric/internal/protocol"
	"golang.org/x/sync/errgroup"
)

func (db *Olric) purgeOldVersions(hkey uint64, name, key string) {
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
		msg := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err := db.requestTo(owner.String(), protocol.OpDeletePrev, msg)
		if err != nil {
			db.log.Printf("[ERROR] Failed to remove purge %s: %s on %s", name, key, owner)
		}
	}
}

func (db *Olric) putKeyVal(hkey uint64, name, key string, value []byte, timeout time.Duration) error {
	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return err
	}
	dm.Lock()
	defer dm.Unlock()

	if db.config.BackupCount != 0 {
		if db.config.BackupMode == AsyncBackupMode {
			db.wg.Add(1)
			go func() {
				defer db.wg.Done()
				err := db.putKeyValBackup(hkey, name, key, value, timeout)
				if err != nil {
					db.log.Printf("[ERROR] Failed to create backup mode in async mode: %v", err)
				}
			}()
		} else {
			err := db.putKeyValBackup(hkey, name, key, value, timeout)
			if err != nil {
				return fmt.Errorf("failed to create backup in sync mode: %v", err)
			}
		}
	}

	var ttl int64
	if timeout.Seconds() != 0 {
		ttl = getTTL(timeout)
	}
	val := &offheap.VData{
		Key:   key,
		TTL:   ttl,
		Value: value,
	}
	err = dm.off.Put(hkey, val)
	if err != nil {
		return err
	}

	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dm.oplog.Put(hkey)
	}
	// TODO: Consider running this at background.
	db.purgeOldVersions(hkey, name, key)
	return nil
}

func (db *Olric) put(name, key string, value []byte, timeout time.Duration) error {
	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		req := &protocol.Message{
			DMap:  name,
			Key:   key,
			Value: value,
		}
		opcode := protocol.OpExPut
		if timeout != nilTimeout {
			opcode = protocol.OpExPutEx
			req.Extra = protocol.PutExExtra{TTL: timeout.Nanoseconds()}
		}
		_, err = db.requestTo(member.String(), opcode, req)
		return err
	}
	return db.putKeyVal(hkey, name, key, value, timeout)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key. It's thread-safe.
// The key has to be string. Value type is arbitrary. It is safe to modify the contents of the arguments after Put returns but not before.
func (dm *DMap) PutEx(key string, value interface{}, timeout time.Duration) error {
	val, err := dm.db.serializer.Marshal(value)
	if err != nil {
		return err
	}
	return dm.db.put(dm.name, key, val, timeout)
}

// Put sets the value for the given key. It overwrites any previous value for that key and it's thread-safe.
// The key has to be string. Value type is arbitrary. It is safe to modify the contents of the arguments after Put returns but not before.
func (dm *DMap) Put(key string, value interface{}) error {
	return dm.PutEx(key, value, nilTimeout)
}

func (db *Olric) exPutOperation(req *protocol.Message) *protocol.Message {
	err := db.put(req.DMap, req.Key, req.Value, nilTimeout)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) exPutExOperation(req *protocol.Message) *protocol.Message {
	ttl := req.Extra.(protocol.PutExExtra).TTL
	err := db.put(req.DMap, req.Key, req.Value, time.Duration(ttl))
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) putBackupOperation(req *protocol.Message) *protocol.Message {
	// TODO: We may need to check backup ownership
	hkey := db.getHKey(req.DMap, req.Key)
	dm, err := db.getBackupDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	var ttl int64
	if req.Extra != nil {
		if req.Extra.(protocol.PutExExtra).TTL != 0 {
			tmp := time.Duration(req.Extra.(protocol.PutExExtra).TTL)
			ttl = getTTL(tmp)
		}
	}
	vdata := &offheap.VData{
		Key:   req.Key,
		TTL:   ttl,
		Value: req.Value,
	}

	err = dm.off.Put(hkey, vdata)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dm.oplog.Put(hkey)
	}
	return req.Success()
}

func (db *Olric) putKeyValBackup(hkey uint64, name, key string, value []byte, timeout time.Duration) error {
	memCount := db.discovery.numMembers()
	backupCount := calcMaxBackupCount(db.config.BackupCount, memCount)
	backupOwners := db.getBackupPartitionOwners(hkey)
	if len(backupOwners) > backupCount {
		backupOwners = backupOwners[len(backupOwners)-backupCount:]
	}

	if len(backupOwners) == 0 {
		// There is no backup owner, return nil.
		return nil
	}

	var successful int32
	var g errgroup.Group
	for _, backup := range backupOwners {
		mem := backup
		g.Go(func() error {
			// TODO: We may need to retry with backoff
			msg := &protocol.Message{
				DMap:  name,
				Key:   key,
				Value: value,
			}
			if timeout != nilTimeout {
				msg.Extra = protocol.PutExExtra{TTL: timeout.Nanoseconds()}
			}
			_, err := db.requestTo(mem.String(), protocol.OpPutBackup, msg)
			if err != nil {
				db.log.Printf("[ERROR] Failed to put backup hkey: %s on %s", mem, err)
				return err
			}
			atomic.AddInt32(&successful, 1)
			return nil
		})
	}
	werr := g.Wait()
	// Return nil if one of the backup nodes has the key/value pair, at least.
	// Active anti-entropy system will repair the failed backup node.
	if atomic.LoadInt32(&successful) >= 1 {
		return nil
	}
	return werr
}
