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
	"math/rand"
	"sync"
	"time"
)

func (db *OlricDB) evictKeysAtBackground() {
	defer db.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.evictKeys()
		}
	}
}

func (db *OlricDB) evictKeys() {
	partID := uint64(rand.Intn(int(db.config.PartitionCount)))
	part := db.partitions[partID]
	// Disable writes on this partition
	part.RLock()

	// If the partition is empty, quit immediately.
	if len(part.m) == 0 {
		part.RUnlock()
		return
	}

	// Picks 20 map objects randomly to check out expired keys. Then waits until all the goroutines done.
	var wg sync.WaitGroup
	dcount := 0
	for name, dm := range part.m {
		dcount++
		if dcount >= 20 {
			break
		}
		wg.Add(1)
		go db.scanDMapForEviction(partID, name, dm, &wg)
	}
	part.RUnlock()
	wg.Wait()
}

func (db *OlricDB) scanDMapForEviction(partID uint64, name string, dm *dmap, wg *sync.WaitGroup) {
	/*
		1- Test 20 random keys from the set of keys with an associated expire.
		2- Delete all the keys found expired.
		3- If more than 25% of keys were expired, start again from step 1.
	*/
	defer wg.Done()
	dm.Lock()
	defer dm.Unlock()
	var totalCount = 0
	var maxKcount = 20
	janitor := func() bool {
		dcount, kcount := 0, 0
		for hkey, vdata := range dm.d {
			kcount++
			if kcount >= maxKcount {
				break
			}
			if isKeyExpired(vdata.TTL) {
				err := db.delKeyVal(dm, hkey, name)
				if err != nil {
					db.logger.Printf("[ERROR] Failed to delete expired hkey: %d on DMap: %s: %v", hkey, name, err)
					continue
				}
				dcount++
			}
		}
		totalCount += dcount
		return dcount >= maxKcount/4
	}
	defer func() {
		db.logger.Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
	}()
	for {
		select {
		case <-db.ctx.Done():
			return
		default:
		}
		if !janitor() {
			return
		}
	}
}
