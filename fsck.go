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
	"encoding/gob"
	"sync"
)

type dmapbox struct {
	PartID  uint64
	Name    string
	Payload map[uint64]vdata
}

func (db *OlricDB) moveBackupDmaps(bpart *partition, backups []host, wg *sync.WaitGroup) {
	defer wg.Done() // local wg for this fsck call

	bpart.RLock()
	defer bpart.RUnlock()

	// TODO: We may need to implement worker to limit concurrency. If the dmap count is too big, the following
	// code may cause CPU starvation.
	for _, backup := range backups {
		for name, dm := range bpart.m {
			wg.Add(1)
			go db.moveDmap(bpart, name, dm, backup, wg)
		}
	}
}

func (db *OlricDB) moveDmaps(part *partition, owner host, wg *sync.WaitGroup) {
	defer wg.Done() // local wg for this fsck call
	part.RLock()
	defer part.RUnlock()

	// TODO: We may need to implement worker to limit concurrency. If the dmap count is too big, the following
	// code may cause CPU starvation.
	for name, dm := range part.m {
		wg.Add(1)
		go db.moveDmap(part, name, dm, owner, wg)
	}
}

func (db *OlricDB) moveDmap(part *partition, name string, dm *dmap, owner host, wg *sync.WaitGroup) {
	defer wg.Done()
	dm.Lock()
	defer dm.Unlock()

	if !part.backup {
		if dm.locker.length() != 0 {
			db.logger.Printf("[DEBUG] Lock found on %s. moveDmap has been cancelled", name)
			return
		}
	}
	data := &dmapbox{
		PartID:  part.id,
		Name:    name,
		Payload: dm.d,
	}
	// To encode nil values, register struct{}{}
	registerValueType(struct{}{})

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(data)
	if err != nil {
		db.logger.Printf("[ERROR] Failed to encode dmap. partID: %d, name: %s, error: %v", data.PartID, data.Name, err)
		return
	}
	if !part.backup {
		err = db.transport.moveDmap(buf.Bytes(), owner)
	} else {
		err = db.transport.moveBackupDmap(buf.Bytes(), owner)
	}
	if err != nil {
		db.logger.Printf("[ERROR] Failed to move dmap. partID: %d, name: %s, error: %v", data.PartID, data.Name, err)
		return
	}

	part.Lock()
	// Delete moved dmap object. the gc will free the allocated memory.
	delete(part.m, name)
	part.Unlock()
}

func (db *OlricDB) mergeDMaps(part *partition, data *dmapbox) {
	part.Lock()
	defer part.Unlock()

	dm, ok := part.m[data.Name]
	if !ok {
		dm = &dmap{d: data.Payload}
		if !part.backup {
			// Create this on the owners, not backups.
			dm.locker = newLocker()
		}
		part.m[data.Name] = dm
		return
	}
	for hkey, value := range data.Payload {
		_, ok := dm.d[hkey]
		if !ok {
			dm.d[hkey] = value
		}
	}
	part.m[data.Name] = dm
}

func (db *OlricDB) fsck() {
	db.fsckMtx.Lock()
	defer db.fsckMtx.Unlock()

	var wg sync.WaitGroup
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.partitions[partID]
		part.RLock()
		if len(part.m) == 0 {
			part.RUnlock()
			continue
		}
		primaryOwner := part.owners[len(part.owners)-1]
		for _, node := range part.owners[:len(part.owners)-1] {
			if hostCmp(node, db.this) {
				wg.Add(1)
				go db.moveDmaps(part, primaryOwner, &wg)
				break
			}
		}
		part.RUnlock()
	}

	memCount := db.discovery.numMembers()
	backupCount := calcMaxBackupCount(db.config.BackupCount, memCount)
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		bpart := db.backups[partID]
		bpart.RLock()
		if len(bpart.m) == 0 || len(bpart.owners) <= backupCount {
			bpart.RUnlock()
			continue
		}

		backups := bpart.owners[len(bpart.owners)-backupCount:]
		staleBackups := bpart.owners[:len(bpart.owners)-backupCount]
		for _, backup := range staleBackups {
			if hostCmp(backup, db.this) {
				wg.Add(1)
				go db.moveBackupDmaps(bpart, backups, &wg)
				break
			}
		}
		bpart.RUnlock()
	}
	wg.Wait()
}
