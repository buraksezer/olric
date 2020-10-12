// Copyright 2018-2020 Burak Sezer
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
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/storage"
	"golang.org/x/sync/errgroup"
)

func (db *Olric) deleteStaleDMaps() {
	janitor := func(part *partition) {
		part.m.Range(func(name, dm interface{}) bool {
			d := dm.(*dmap)
			d.Lock()
			defer d.Unlock()
			if d.storage.Len() != 0 {
				// Continue scanning.
				return true
			}
			part.m.Delete(name)
			db.log.V(4).Printf("[INFO] Stale dmap (backup: %v) has been deleted: %s on PartID: %d",
				part.backup, name, part.id)
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

func (db *Olric) deleteKeyValFromPreviousOwners(name, key string, owners []discovery.Member) error {
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		req := protocol.NewDMapMessage(protocol.OpDeletePrev)
		req.SetDMap(name)
		req.SetKey(key)
		_, err := db.requestTo(owner.String(), req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Olric) delKeyVal(dm *dmap, hkey uint64, name, key string) error {
	owners := db.getPartitionOwners(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	err := db.deleteKeyValFromPreviousOwners(name, key, owners)
	if err != nil {
		return err
	}

	if db.config.ReplicaCount != 0 {
		err := db.deleteKeyValBackup(hkey, name, key)
		if err != nil {
			return err
		}
	}
	err = dm.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		db.wg.Add(1)
		go db.compactTables(dm)
		err = nil
	}

	// Delete it from access log if everything is ok.
	// If we delete the hkey when err is not nil, LRU/MaxIdleDuration may not work properly.
	if err == nil {
		dm.deleteAccessLog(hkey)
	}
	return err
}

func (db *Olric) deleteKey(name, key string) error {
	member, hkey := db.findPartitionOwner(name, key)
	if !cmpMembersByName(member, db.this) {
		req := protocol.NewDMapMessage(protocol.OpDelete)
		req.SetDMap(name)
		req.SetKey(key)
		_, err := db.requestTo(member.String(), req)
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

func (db *Olric) exDeleteOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	err := db.deleteKey(req.DMap(), req.Key())
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) deletePrevOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	dm, err := db.getDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.Lock()
	defer dm.Unlock()

	err = dm.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		db.wg.Add(1)
		go db.compactTables(dm)
		err = nil
	}
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) deleteBackupOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	hkey := db.getHKey(req.DMap(), req.Key())
	dm, err := db.getBackupDMap(req.DMap(), hkey)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	dm.Lock()
	defer dm.Unlock()

	err = dm.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		db.wg.Add(1)
		go db.compactTables(dm)
		err = nil
	}
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}

func (db *Olric) deleteKeyValBackup(hkey uint64, name, key string) error {
	backupOwners := db.getBackupPartitionOwners(hkey)
	var g errgroup.Group
	for _, backup := range backupOwners {
		mem := backup
		g.Go(func() error {
			// TODO: Add retry with backoff
			req := protocol.NewDMapMessage(protocol.OpDeleteBackup)
			req.SetDMap(name)
			req.SetKey(key)
			_, err := db.requestTo(mem.String(), req)
			if err != nil {
				db.log.V(3).Printf("[ERROR] Failed to delete backup key/value on %s: %s", name, err)
			}
			return err
		})
	}
	return g.Wait()
}
