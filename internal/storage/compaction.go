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

package storage

import (
	"fmt"
	"log"
)

func (s *Storage) pruneEmptyTables() {
	var tmp []*table
	for _, t := range s.tables {
		if len(t.hkeys) == 0 {
			fmt.Println("Pruned table", t.allocated, t.inuse, t.garbage, cap(t.memory))
			t = nil
			continue
		}
		tmp = append(tmp, t)
	}
	s.tables = tmp
}

func (s *Storage) CompactTables() bool {
	if len(s.tables) == 1 {
		return true
	}

	defer s.pruneEmptyTables()

	var total int

	latest := s.tables[len(s.tables)-1]
	for _, old := range s.tables[:len(s.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			entry, _ := old.getRaw(hkey)
			err := latest.putRaw(hkey, entry)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				fmt.Println(">> [COMPACTION] New table size", MiB(uint64(s.Inuse() * 2)))
				nt := newTable(1<<24)
				s.tables = append(s.tables, nt)
				return false
			}
			if err != nil {
				log.Printf("[ERROR] Failed to compact tables. HKey: %d: %v", hkey, err)
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
	return true
}
