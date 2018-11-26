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

package olric

import (
	"context"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/snapshot"
	"golang.org/x/sync/errgroup"
)

func (db *Olric) destroyDMap(name string) error {
	<-db.bcx.Done()
	if db.bcx.Err() == context.DeadlineExceeded {
		return ErrOperationTimeout
	}

	var g errgroup.Group
	for _, item := range db.discovery.getMembers() {
		addr := item.String()
		g.Go(func() error {
			msg := &protocol.Message{
				DMap: name,
			}
			_, err := db.requestTo(addr, protocol.OpDestroyDMap, msg)
			if err != nil {
				db.log.Printf("[ERROR] Failed to destroy dmap:%s on %s", name, addr)
			}
			return err
		})
	}
	return g.Wait()
}

// Destroy flushes the given DMap on the cluster. You should know that there
// is no global lock on DMaps. So if you call Put/PutEx and Destroy methods
// concurrently on the cluster, Put/PutEx calls may set new values to the DMap.
func (dm *DMap) Destroy() error {
	return dm.db.destroyDMap(dm.name)
}

func (db *Olric) exDestroyOperation(req *protocol.Message) *protocol.Message {
	err := db.destroyDMap(req.DMap)
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}
	return req.Success()
}

func (db *Olric) destroyDMapOperation(req *protocol.Message) *protocol.Message {
	// This is very similar with rm -rf. Destroys given dmap on the cluster
	destroy := func(part *partition) error {
		tmp, ok := part.m.Load(req.DMap)
		if !ok {
			return nil
		}
		dm := tmp.(*dmap)
		if err := dm.str.Close(); err != nil {
			return err
		}
		if db.config.OperationMode == OpInMemoryWithSnapshot {
			dkey := snapshot.PrimaryDMapKey
			if part.backup {
				dkey = snapshot.BackupDMapKey
			}
			if err := db.snapshot.DestroyDMap(dkey, part.id, req.DMap); err != nil {
				return err
			}
		}
		part.m.Delete(req.DMap)
		return nil
	}
	// Fail early. The caller may want to call again if one of the steps have failed.
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		// Delete primary copies
		part := db.partitions[partID]
		if err := destroy(part); err != nil {
			return req.Error(protocol.StatusInternalServerError, err)
		}
		// Delete from Backups
		if db.config.BackupCount != 0 {
			bpart := db.backups[partID]
			if err := destroy(bpart); err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
		}
	}
	return req.Success()
}
