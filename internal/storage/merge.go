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

package storage

import (
	"log"
	"sync/atomic"
	"time"
)

func (s *Storage) mergeTables() {
	defer s.wg.Done()
	defer atomic.StoreInt32(&s.merging, 0)
	// Run immediately. The ticker will trigger that function
	// every 100 milliseconds to prevent blocking the storage instance.
	if done := s.chunkedMergeTables(); done {
		// Fragmented tables are merged. Quit.
		return
	}
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if done := s.chunkedMergeTables(); done {
				// Fragmented tables are merged. Quit.
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) chunkedMergeTables() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tables) == 1 {
		return true
	}

	var total int
	fresh := s.tables[len(s.tables)-1]
	for _, old := range s.tables[:len(s.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			vdata, _ := old.getRaw(hkey)
			err := fresh.putRaw(hkey, vdata)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				nt, err := newTable(fresh.allocated * 2)
				if err != nil {
					log.Printf("[ERROR] storage: failed to create new table: %v", err)
					return false
				}
				s.tables = append(s.tables, nt)
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
	tmp := []*table{s.tables[len(s.tables)-1]}
	for _, t := range s.tables[:len(s.tables)-1] {
		if len(t.hkeys) == 0 {
			err := t.close()
			if err != nil {
				log.Printf("[ERROR] storage: failed to close table: %v", err)
			}
			continue
		}
		tmp = append(tmp, t)
	}
	s.tables = tmp
	return true
}
