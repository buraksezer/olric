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

func (d *Docstore) CompactTables() bool {
	if len(d.tables) == 1 {
		return true
	}

	var total int
	fresh := d.tables[len(d.tables)-1]
	for _, old := range d.tables[:len(d.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			entry, _ := old.getRaw(hkey)
			err := fresh.putRaw(hkey, entry)
			if err == errNotEnoughSpace {
				// Create a new table and put the new k/v pair in it.
				nt := newTable(d.Inuse() * 2)
				d.tables = append(d.tables, nt)
				return false
			}
			if err != nil {
				log.Printf("[ERROR] Failed to compact tables. HKey: %d: %v", hkey, err)
			}

			// Dont check the returned val, it'd useless because
			// we are sure that the key is already there.
			old.delete(hkey)
			total++
			if total > 1000 {
				// It'd enough. Don't block the instance.
				return false
			}
		}
	}

	// Remove empty tables. Keep the last table.
	tmp := []*table{d.tables[len(d.tables)-1]}
	for _, t := range d.tables[:len(d.tables)-1] {
		if len(t.hkeys) == 0 {
			continue
		}
		tmp = append(tmp, t)
	}
	d.tables = tmp
	return true
}
