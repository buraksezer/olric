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
	"sync"
	"sync/atomic"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/snapshot"
	"github.com/vmihailenco/msgpack"
)

type dmapbox struct {
	PartID  uint64
	Name    string
	Payload []byte
}

func (db *Olric) moveBackupDMaps(part *partition, backups []host, wg *sync.WaitGroup) {
	defer wg.Done() // local wg for this fsck call

	// TODO: We may need to implement worker to limit concurrency. If the dmap count is too big, the following
	// code may cause CPU/network IO starvation.
	for _, backup := range backups {
		part.m.Range(func(name, dm interface{}) bool {
			wg.Add(1)
			go db.moveDMap(part, name.(string), dm.(*dmap), backup, wg)
			return true
		})
	}
}

func (db *Olric) moveDMaps(part *partition, owner host, wg *sync.WaitGroup) {
	defer wg.Done() // local wg for this fsck call
	// TODO: We may need to implement worker to limit concurrency. If the dmap count is too large, the following
	// code may cause CPU/network IO starvation.
	part.m.Range(func(name, dm interface{}) bool {
		wg.Add(1)
		go db.moveDMap(part, name.(string), dm.(*dmap), owner, wg)
		return true
	})
}

func (db *Olric) moveDMap(part *partition, name string, dm *dmap, owner host, wg *sync.WaitGroup) {
	defer wg.Done()
	dm.Lock()
	defer dm.Unlock()

	if !part.backup {
		if dm.locker.length() != 0 {
			db.log.Printf("[DEBUG] Lock found on %s. moveDMap has been cancelled", name)
			return
		}
	}

	payload, err := dm.off.Export()
	if err != nil {
		db.log.Printf("[ERROR] Failed to call Export on dmap. partID: %d, name: %s, error: %v", part.id, name, err)
		return
	}
	data := &dmapbox{
		PartID:  part.id,
		Name:    name,
		Payload: payload,
	}
	value, err := msgpack.Marshal(data)
	if err != nil {
		db.log.Printf("[ERROR] Failed to encode dmap. partID: %d, name: %s, error: %v", data.PartID, data.Name, err)
		return
	}
	var opcode protocol.OpCode
	if !part.backup {
		opcode = protocol.OpMoveDMap
	} else {
		opcode = protocol.OpBackupMoveDMap
	}
	req := &protocol.Message{
		Value: value,
	}
	_, err = db.requestTo(owner.String(), opcode, req)
	if err != nil {
		db.log.Printf("[ERROR] Failed to move dmap. partID: %d, name: %s, error: %v", data.PartID, data.Name, err)
		return
	}

	// Delete moved dmap object. the gc will free the allocated memory.
	part.m.Delete(name)
	atomic.AddInt32(&part.count, -1)
	err = dm.off.Close()
	if err != nil {
		db.log.Printf("[ERROR] Failed to close offheap instance. partID: %d, name: %s, error: %v", data.PartID, data.Name, err)
	}
	if db.config.OperationMode == OpInMemoryWithSnapshot {
		dkey := snapshot.PrimaryDMapKey
		if part.backup {
			dkey = snapshot.BackupDMapKey
		}
		err = db.snapshot.DestroyDMap(dkey, part.id, name)
		if err != nil {
			db.log.Printf(
				"[ERROR] Failed to destroy moved DMap instance on BadgerDB. PartID(backup: %t): %d, name: %s, error: %v",
				part.backup, data.PartID, data.Name, err,
			)
		}
	}
}

func (db *Olric) mergeDMaps(part *partition, data *dmapbox) error {
	oh, err := offheap.Import(data.Payload)
	if err != nil {
		return err
	}

	tmp, ok := part.m.Load(data.Name)
	if !ok {
		dm := &dmap{off: oh}
		if !part.backup {
			// Create this on the owners, not backups.
			dm.locker = newLocker()
		}
		part.m.Store(data.Name, dm)
		return nil
	}

	dm := tmp.(*dmap)
	dm.Lock()
	defer dm.Unlock()

	var merr error
	oh.Range(func(hkey uint64, vdata *offheap.VData) bool {
		if !dm.off.Check(hkey) {
			merr = dm.off.Put(hkey, vdata)
			if merr != nil {
				return false
			}
		}
		return true
	})
	return merr
}

func (db *Olric) fsck() {
	db.fsckMx.Lock()
	defer db.fsckMx.Unlock()

	var wg sync.WaitGroup
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		if atomic.LoadInt32(&part.count) == 0 {
			continue
		}
		part.RLock()
		primaryOwner := part.owners[len(part.owners)-1]
		for _, node := range part.owners[:len(part.owners)-1] {
			if hostCmp(node, db.this) {
				wg.Add(1)
				go db.moveDMaps(part, primaryOwner, &wg)
				break
			}
		}
		part.RUnlock()
	}

	memCount := db.discovery.numMembers()
	backupCount := calcMaxBackupCount(db.config.BackupCount, memCount)
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		bpart := db.backups[partID]
		if atomic.LoadInt32(&bpart.count) == 0 {
			continue
		}

		bpart.RLock()
		if len(bpart.owners) <= backupCount {
			bpart.RUnlock()
			continue
		}

		backups := bpart.owners[len(bpart.owners)-backupCount:]
		staleBackups := bpart.owners[:len(bpart.owners)-backupCount]
		for _, backup := range staleBackups {
			if hostCmp(backup, db.this) {
				wg.Add(1)
				go db.moveBackupDMaps(bpart, backups, &wg)
				break
			}
		}
		bpart.RUnlock()
	}
	wg.Wait()
}

func (db *Olric) moveBackupDMapOperation(req *protocol.Message) *protocol.Message {
	dbox := &dmapbox{}
	err := msgpack.Unmarshal(req.Value, dbox)
	if err != nil {
		db.log.Printf("[ERROR] Failed to unmarshal dmap for backup: %v", err)
		return req.Error(protocol.StatusInternalServerError, err)
	}
	part := db.backups[dbox.PartID]
	part.RLock()
	if len(part.owners) == 0 {
		part.RUnlock()
		panic("partition owners list cannot be empty")
	}
	part.RUnlock()
	// TODO: Check partition owner here!
	err = db.mergeDMaps(part, dbox)
	if err != nil {
		db.log.Printf("[ERROR] Failed to merge dmap for backup: %v", err)
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) moveDMapOperation(req *protocol.Message) *protocol.Message {
	dbox := &dmapbox{}
	err := msgpack.Unmarshal(req.Value, dbox)
	if err != nil {
		db.log.Printf("[ERROR] Failed to unmarshal dmap for backup: %v", err)
		return req.Error(protocol.StatusInternalServerError, err)
	}

	part := db.partitions[dbox.PartID]
	part.RLock()
	if len(part.owners) == 0 {
		part.RUnlock()
		panic("partition owners list cannot be empty")
	}
	part.RUnlock()
	// TODO: Check partition owner here!
	err = db.mergeDMaps(part, dbox)
	if err != nil {
		db.log.Printf("[ERROR] Failed to merge dmap: %v", err)
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}
