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
	"math/rand"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/storage"
)

func (db *Olric) evictKeysAtBackground() {
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

func (db *Olric) evictKeys() {
	partID := uint64(rand.Intn(int(db.config.PartitionCount)))
	part := db.partitions[partID]

	var wg sync.WaitGroup
	part.m.Range(func(name, tmp interface{}) bool {
		dm := tmp.(*dmap)
		// Picks 20 map objects randomly to check out expired keys. Then waits until all the goroutines done.
		dcount := 0
		dcount++
		if dcount >= 20 {
			return false
		}
		wg.Add(1)
		go db.scanDMapForEviction(partID, name.(string), dm, &wg)
		return true
	})

	wg.Wait()
}

func (db *Olric) scanDMapForEviction(partID uint64, name string, dm *dmap, wg *sync.WaitGroup) {
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
		dm.str.Range(func(hkey uint64, vdata *storage.VData) bool {
			kcount++
			if kcount >= maxKcount {
				return false
			}
			if isKeyExpired(vdata.TTL) {
				err := db.delKeyVal(dm, hkey, name, vdata.Key)
				if err != nil {
					db.log.Printf("[ERROR] Failed to delete expired hkey: %d on DMap: %s: %v", hkey, name, err)
					return true
				}
				dcount++
			}
			return true
		})
		totalCount += dcount
		return dcount >= maxKcount/4
	}
	defer func() {
		db.log.Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
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
