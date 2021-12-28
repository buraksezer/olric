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

// pruneStaleTables removes stale tables from the table slice and make them available
// for garbage collection. It always keeps the latest table.
func (kv *KVStore) pruneStaleTables() {
	var staleTables int

	// Calculate how many tables are stale and eligible to remove.
	// Always keep the latest table.
	for _, t := range kv.tables[:len(kv.tables)-1] {
		if len(t.hkeys) == 0 {
			staleTables++
		}
	}

	for i := 0; i < staleTables; i++ {
		for i, t := range kv.tables {
			// See https://github.com/golang/go/wiki/SliceTricks
			if len(t.hkeys) != 0 {
				continue
			}
			copy(kv.tables[i:], kv.tables[i+1:])
			kv.tables[len(kv.tables)-1] = nil // or the zero value of T
			kv.tables = kv.tables[:len(kv.tables)-1]
			break
		}
	}
}

func (kv *KVStore) Compaction() (bool, error) {
	if len(kv.tables) == 1 {
		return true, nil
	}

	defer kv.pruneStaleTables()

	var total int
	latest := kv.tables[len(kv.tables)-1]
	for _, prev := range kv.tables[:len(kv.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range prev.hkeys {
			entry, _ := prev.getRaw(hkey)
			err := latest.putRaw(hkey, entry)
			if errors.Is(err, errNotEnoughSpace) {
				// Create a new table and put the new k/v pair in it.
				ntSize := kv.calculateTableSize(len(entry))
				nt := newTable(ntSize)
				kv.tables = append(kv.tables, nt)
				return false, nil
			}
			if err != nil {
				// log this error and continue
				return false, fmt.Errorf("put command failed: HKey: %d: %w", hkey, err)
			}

			// Dont check the returned val, it's useless because
			// we are sure that the key is already there.
			prev.delete(hkey)
			total++
			if total > 1000 {
				// It's enough. Don't block the instance.
				return false, nil
			}
		}
	}

	return true, nil
}
