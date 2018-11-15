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

package offheap

import (
	"log"
	"sync/atomic"
	"time"
)

func (o *Offheap) mergeTables() {
	defer o.wg.Done()
	defer atomic.StoreInt32(&o.merging, 0)
	// Run immediately. The ticker will trigger that function
	// every 100 milliseconds to prevent blocking the offheap instance.
	if done := o.chunkedMergeTables(); done {
		// Fragmented tables are merged. Quit.
		return
	}
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if done := o.chunkedMergeTables(); done {
				// Fragmented tables are merged. Quit.
				return
			}
		case <-o.ctx.Done():
			return
		}
	}
}

func (o *Offheap) chunkedMergeTables() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.tables) == 1 {
		return true
	}

	var total int
	fresh := o.tables[len(o.tables)-1]
	for _, old := range o.tables[:len(o.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			vdata, _ := old.getRaw(hkey)
			err := fresh.putRaw(hkey, vdata)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				nt, err := newTable(fresh.allocated * 2)
				if err != nil {
					log.Printf("[ERROR] offheap: failed to create new table: %v", err)
					return false
				}
				o.tables = append(o.tables, nt)
				return false
			}
			if err != nil {
				log.Printf("[ERROR] Failed to merge tables. HKey: %d: %v", hkey, err)
			}

			// Dont check the returned val, it's useless because
			// we are sure that the key is already there.
			old.delete(hkey)
			total++
			if total > 1000 {
				// It's enough. Don't block the instance.
				return false
			}
		}
	}

	// Remove empty tables. Keep the last table.
	tmp := []*table{o.tables[len(o.tables)-1]}
	for _, t := range o.tables[:len(o.tables)-1] {
		if len(t.hkeys) == 0 {
			err := t.close()
			if err != nil {
				log.Printf("[ERROR] offheap: failed to close table: %v", err)
			}
			continue
		}
		tmp = append(tmp, t)
	}
	o.tables = tmp
	return true
}
