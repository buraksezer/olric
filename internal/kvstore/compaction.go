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
	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

// pruneStaleTables removes stale tables from the table slice and make them available
// for garbage collection. It always keeps the latest table.
/*func (k *KVStore) pruneStaleTables() {
	var staleTables int

	// Calculate how many tables are stale and eligible to remove.
	// Always keep the latest table.
	for _, t := range k.tables[:len(k.tables)-1] {
		if len(t.hkeys) == 0 {
			staleTables++
		}
	}

	for i := 0; i < staleTables; i++ {
		for i, t := range k.tables {
			// See https://github.com/golang/go/wiki/SliceTricks
			if len(t.hkeys) != 0 {
				continue
			}
			copy(k.tables[i:], k.tables[i+1:])
			k.tables[len(k.tables)-1] = nil // or the zero value of T
			k.tables = k.tables[:len(k.tables)-1]
			break
		}
	}
}*/

/*func (k *KVStore) Compaction() (bool, error) {
	defer k.pruneStaleTables()

	var total int
	latest := k.tables[len(k.tables)-1]
	for _, prev := range k.tables[:len(k.tables)-1] {
		// Removing keys while iterating on map is totally safe in Go.
		for hkey := range prev.hkeys {
			entry, _ := prev.getRaw(hkey)
			err := latest.putRaw(hkey, entry)
			if errors.Is(err, table.errNotEnoughSpace) {
				// Create a new table and put the new k/v pair in it.
				nt := table.newTable(k.Stats().Inuse * 2)
				k.tables = append(k.tables, nt)
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
}*/

func (k *KVStore) evictTable(t *table.Table) error {
	var total int
	var evictErr error
	t.Range(func(hkey uint64, e storage.Entry) bool {
		entry, _ := t.GetRaw(hkey)
		err := k.PutRaw(hkey, entry)
		if errors.Is(err, table.ErrNotEnoughSpace) {
			err := k.makeTable()
			if err != nil {
				evictErr = err
				return false
			}
			// try again
			return false
		}
		if err != nil {
			// log this error and continue
			evictErr = fmt.Errorf("put command failed: HKey: %d: %w", hkey, err)
			return false
		}

		// Dont check the returned val, it's useless because
		// we are sure that the key is already there.
		err = t.Delete(hkey)
		if err == table.ErrHKeyNotFound {
			err = nil
		}
		if err != nil {
			evictErr = err
			return false
		}
		total++
		if total > 1000 {
			// It's enough. Don't block the instance.
			return false
		}

		return true
	})

	return evictErr
}

func (k *KVStore) Compaction() (bool, error) {
	for _, t := range k.tables {
		s := t.Stats()
		if float64(s.Allocated)*maxGarbageRatio <= float64(s.Garbage) {
			err := k.evictTable(t)
			if err != nil {
				return false, err
			}

			// Continue scanning
			return false, nil
		}
	}

	return true, nil
}
