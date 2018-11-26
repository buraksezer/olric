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
	"sync/atomic"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/snapshot"
	"golang.org/x/sync/errgroup"
)

func (db *Olric) deleteStaleDMapsAtBackground() {
	defer db.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.deleteStaleDMaps()
		}
	}
}

func (db *Olric) deleteStaleDMaps() {
	janitor := func(part *partition) {
		part.m.Range(func(name, dm interface{}) bool {
			d := dm.(*dmap)
			d.Lock()
			defer d.Unlock()
			if d.str.Len() != 0 {
				// Continue scanning.
				return true
			}
			err := d.str.Close()
			if err != nil {
				db.log.Printf("[ERROR] Failed to close storage instance: %s on PartID: %d: %v",
					name, part.id, err)
				return true
			}
			// Unregister DMap from snapshot.
			if db.config.OperationMode == OpInMemoryWithSnapshot {
				dkey := snapshot.PrimaryDMapKey
				if part.backup {
					dkey = snapshot.BackupDMapKey
				}
				err = db.snapshot.UnregisterDMap(dkey, part.id, name.(string))
				if err != nil {
					db.log.Printf("[ERROR] Failed to unregister dmap from snapshot %s on PartID: %d: %v",
						name, part.id, err)
					// Try again later.
					return true
				}
			}
			part.m.Delete(name)
			atomic.AddInt32(&part.count, -1)
			db.log.Printf("[DEBUG] Stale DMap has been deleted: %s on PartID: %d", name, part.id)
			return true
		})
	}
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		// Clean stale dmaps on partition table
		part := db.partitions[partID]
		janitor(part)
		// Clean stale dmaps on backup partition table
		backup := db.backups[partID]
		janitor(backup)
	}
}

func (db *Olric) delKeyVal(dm *dmap, hkey uint64, name, key string) error {
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
		msg := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err := db.requestTo(owner.String(), protocol.OpDeletePrev, msg)
		if err != nil {
			return err
		}
	}
	if db.config.BackupCount != 0 {
		err := db.deleteKeyValBackup(hkey, name, key)
		if err != nil {
			return err
		}
	}
	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dm.oplog.Delete(hkey)
	}
	return dm.str.Delete(hkey)
}

func (db *Olric) deleteKey(name, key string) error {
	member, hkey, err := db.locateKey(name, key)
	if err != nil {
		return err
	}
	if !hostCmp(member, db.this) {
		msg := &protocol.Message{
			DMap: name,
			Key:  key,
		}
		_, err := db.requestTo(member.String(), protocol.OpExDelete, msg)
		return err
	}

	dm, err := db.getDMap(name, hkey)
	if err != nil {
		return err
	}
	dm.Lock()
	defer dm.Unlock()
	return db.delKeyVal(dm, hkey, name, key)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(key string) error {
	return dm.db.deleteKey(dm.name, key)
}

func (db *Olric) exDeleteOperation(req *protocol.Message) *protocol.Message {
	err := db.deleteKey(req.DMap, req.Key)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) deletePrevOperation(req *protocol.Message) *protocol.Message {
	hkey := db.getHKey(req.DMap, req.Key)
	dm, err := db.getDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	dm.Lock()
	defer dm.Unlock()

	err = dm.str.Delete(hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dm.oplog.Delete(hkey)
	}
	return req.Success()
}

func (db *Olric) deleteBackupOperation(req *protocol.Message) *protocol.Message {
	// TODO: We may need to check backup ownership
	hkey := db.getHKey(req.DMap, req.Key)
	dm, err := db.getBackupDMap(req.DMap, hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	dm.Lock()
	defer dm.Unlock()

	err = dm.str.Delete(hkey)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dm.oplog.Delete(hkey)
	}
	return req.Success()
}

func (db *Olric) deleteKeyValBackup(hkey uint64, name, key string) error {
	backupOwners := db.getBackupPartitionOwners(hkey)
	var g errgroup.Group
	for _, backup := range backupOwners {
		mem := backup
		g.Go(func() error {
			// TODO: Add retry with backoff
			req := &protocol.Message{
				DMap: name,
				Key:  key,
			}
			_, err := db.requestTo(mem.String(), protocol.OpDeleteBackup, req)
			if err != nil {
				db.log.Printf("[ERROR] Failed to delete backup key/value on %s: %s", name, err)
			}
			return err
		})
	}
	return g.Wait()
}
