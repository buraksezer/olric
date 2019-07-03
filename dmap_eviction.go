// Copyright 2018-2019 Burak Sezer
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
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/storage"
)

func (db *Olric) evictKeysAtBackground() {
	defer db.wg.Done()

	// Scan keys at background, 10 times per second.
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.wg.Add(1)
			go db.evictKeys()
		}
	}
}

func (db *Olric) evictKeys() {
	defer db.wg.Done()

	dmCount := 0
	partID := uint64(rand.Intn(int(db.config.PartitionCount)))
	part := db.partitions[partID]
	var wg sync.WaitGroup
	part.m.Range(func(name, tmp interface{}) bool {
		dm := tmp.(*dmap)
		// Picks 20 dmap objects randomly to check out expired keys. Then waits until all the goroutines done.
		dmCount++
		if dmCount >= 20 {
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
		From Redis Docs:
			1- Test 20 random keys from the set of keys with an associated expire.
			2- Delete all the keys found expired.
			3- If more than 25% of keys were expired, start again from step 1.
	*/
	defer wg.Done()

	// We need limits to prevent CPU starvation. delKeyVal does some network operation
	// to delete keys from the backup nodes and the previous owners.
	var maxKeyCount = 20
	var maxTotalCount = 100
	var totalCount = 0

	dm.Lock()
	defer dm.Unlock()

	janitor := func() bool {
		if totalCount > maxTotalCount {
			// Release the lock. Eviction will be triggered again.
			return false
		}

		count, keyCount := 0, 0
		dm.str.Range(func(hkey uint64, vdata *storage.VData) bool {
			keyCount++
			if keyCount >= maxKeyCount {
				// this means 'break'.
				return false
			}
			if isKeyExpired(vdata.TTL) || dm.isKeyIdle(hkey) {
				err := db.delKeyVal(dm, hkey, name, vdata.Key)
				if err != nil {
					// It will be tried again.
					db.log.Printf("[ERROR] Failed to delete expired hkey: %d on DMap: %s: %v", hkey, name, err)
					return true // this means 'continue'
				}
				count++
			}
			return true
		})
		totalCount += count
		return count >= maxKeyCount/4
	}
	defer func() {
		if totalCount > 0 {
			db.log.Printf("[DEBUG] Evicted key count is %d on PartID: %d", totalCount, partID)
		}
	}()
	for {
		select {
		case <-db.ctx.Done():
			// The server has gone.
			return
		default:
		}
		// Call janitor again until it returns false.
		if !janitor() {
			return
		}
	}
}

func (dm *dmap) updateAccessLog(hkey uint64) {
	if dm.cache == nil || dm.cache.accessLog == nil {
		// Fail early. This's useful to avoid checking the configuration everywhere.
		return
	}
	dm.cache.Lock()
	defer dm.cache.Unlock()
	dm.cache.accessLog[hkey] = time.Now().UnixNano()
}

func (dm *dmap) deleteAccessLog(hkey uint64) {
	if dm.cache == nil || dm.cache.accessLog == nil {
		return
	}
	dm.cache.Lock()
	defer dm.cache.Unlock()
	delete(dm.cache.accessLog, hkey)
}

func (dm *dmap) isKeyIdle(hkey uint64) bool {
	if dm.cache == nil || dm.cache.accessLog == nil {
		return false
	}
	// Maximum time in seconds for each entry to stay idle in the map.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically.
	dm.cache.RLock()
	defer dm.cache.RUnlock()
	t, ok := dm.cache.accessLog[hkey]
	if !ok {
		return false
	}
	ttl := (dm.cache.maxIdleDuration.Nanoseconds() + t) / 1000000
	return isKeyExpired(ttl)
}

type lruItem struct {
	HKey       uint64
	AccessedAt int64
}

func (db *Olric) evictKeyWithLRU(dm *dmap, name string) error {
	idx := 0
	items := []lruItem{}
	dm.cache.RLock()
	// Pick random items from the distributed map and sort them by accessedAt.
	for hkey, accessedAt := range dm.cache.accessLog {
		if idx >= dm.cache.lruSamples {
			break
		}
		i := lruItem{
			HKey:       hkey,
			AccessedAt: accessedAt,
		}
		items = append(items, i)
	}
	dm.cache.RUnlock()

	if len(items) == 0 {
		return fmt.Errorf("nothing found to expire with LRU")
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AccessedAt < items[j].AccessedAt })
	// Pick the first item to delete. It's the least recently used item in the sample.
	item := items[0]
	vdata, err := dm.str.Get(item.HKey)
	if err != nil {
		return err
	}
	return db.delKeyVal(dm, item.HKey, name, vdata.Key)
}
