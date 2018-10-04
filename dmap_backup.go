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

package olricdb

import (
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
)

func (db *OlricDB) putKeyValBackup(hkey uint64, name string, value []byte, timeout time.Duration) error {
	memCount := db.discovery.numMembers()
	backupCount := calcMaxBackupCount(db.config.BackupCount, memCount)
	backupOwners := db.getBackupPartitionOwners(hkey)
	if len(backupOwners) > backupCount {
		backupOwners = backupOwners[len(backupOwners)-backupCount:]
	}

	if len(backupOwners) == 0 {
		// There is no backup owner, return nil.
		return nil
	}

	var successful int32
	var g errgroup.Group
	for _, backup := range backupOwners {
		mem := backup
		g.Go(func() error {
			// TODO: We may need to retry with backoff
			err := db.transport.putBackup(mem, name, hkey, value, timeout)
			if err != nil {
				db.logger.Printf("[ERROR] Failed to put backup hkey: %s on %s", mem, err)
				return err
			}
			atomic.AddInt32(&successful, 1)
			return nil
		})
	}
	werr := g.Wait()
	// Return nil if one of the backup nodes has the key/value pair, at least.
	// Active anti-entropy system will repair the failed backup node.
	if atomic.LoadInt32(&successful) >= 1 {
		return nil
	}
	return werr
}

func (db *OlricDB) deleteKeyValBackup(hkey uint64, name string) error {
	backupOwners := db.getBackupPartitionOwners(hkey)
	var g errgroup.Group
	for _, backup := range backupOwners {
		mem := backup
		g.Go(func() error {
			// TODO: Add retry with backoff
			err := db.transport.deleteBackup(mem, hkey, name)
			if err != nil {
				db.logger.Printf("[ERROR] Failed to delete backup key/value on %s: %s", name, err)
			}
			return err
		})
	}
	return g.Wait()
}
