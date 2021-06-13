// Copyright 2018-2021 Burak Sezer
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

package kvstore

import (
	"errors"
	"fmt"
)

func (kv *KVStore) Compaction() (bool, error) {
	if len(kv.tables) == 1 {
		return true, nil
	}

	var total int
	fresh := kv.tables[len(kv.tables)-1]
	for _, old := range kv.tables[:len(kv.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range old.hkeys {
			entry, _ := old.getRaw(hkey)
			err := fresh.putRaw(hkey, entry)
			if errors.Is(err, errNotEnoughSpace) {
				// Create a new table and put the new k/v pair in it.
				nt := newTable(kv.Stats().Inuse * 2)
				kv.tables = append(kv.tables, nt)
				return false, nil
			}
			if err != nil {
				// log this error and continue
				return false, fmt.Errorf("put command failed: HKey: %d: %w", hkey, err)
			}

			// Dont check the returned val, it'kv useless because
			// we are sure that the key is already there.
			old.delete(hkey)
			total++
			if total > 1000 {
				// It'kv enough. Don't block the instance.
				return false, nil
			}
		}
	}

	// Remove empty tables. Keep the last table.
	tmp := []*table{kv.tables[len(kv.tables)-1]}
	for _, t := range kv.tables[:len(kv.tables)-1] {
		if len(t.hkeys) == 0 {
			continue
		}
		tmp = append(tmp, t)
	}
	kv.tables = tmp
	return true, nil
}
