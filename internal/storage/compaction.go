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
	"log"
)

// pruneStaleTables removes stale tables from the table slice and make them available
// for garbage collection. It always keeps the latest table.
func (s *Storage) pruneStaleTables() {
	var staleTables int

	// Calculate how many tables are stale and eligible to remove.
	// Always keep the latest table.
	for _, t := range s.tables[:len(s.tables)-1] {
		if len(t.hkeys) == 0 {
			staleTables++
		}
	}

	for i := 0; i < staleTables; i++ {
		for i, t := range s.tables {
			// See https://github.com/golang/go/wiki/SliceTricks
			if len(t.hkeys) != 0 {
				continue
			}
			copy(s.tables[i:], s.tables[i+1:])
			s.tables[len(s.tables)-1] = nil // or the zero value of T
			s.tables = s.tables[:len(s.tables)-1]
			break
		}
	}
}

func (s *Storage) CompactTables() bool {
	if len(s.tables) == 1 {
		return true // break
	}

	defer s.pruneStaleTables()

	var deleted int
	latest := s.tables[len(s.tables)-1]
	for _, prev := range s.tables[:len(s.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range prev.hkeys {
			entry, _ := prev.getRaw(hkey)
			err := latest.putRaw(hkey, entry)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				nt := newTable(s.Inuse() * 2)
				s.tables = append(s.tables, nt)
				return false // means continue
			}
			if err != nil {
				log.Printf("[ERROR] Failed to compact tables. HKey: %d: %v", hkey, err)
			}

			// Dont check the returned val, it's useless because
			// we are sure that the key is already there.
			prev.delete(hkey)
			deleted++
			if deleted > 1000 {
				// It's enough. Don't block the instance.
				return false // means continue
			}
		}
	}

	return true // means break
}
