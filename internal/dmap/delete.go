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

package dmap

import (
	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/storage"
	"golang.org/x/sync/errgroup"
)

func (dm *DMap) deleteOnPreviousOwner(key string) error {
	hkey := partitions.HKey(dm.name, key)
	f, err := dm.getFragment(hkey, partitions.PRIMARY)
	if err != nil {
		return err
	}

	err = f.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		dm.s.wg.Add(1)
		go dm.s.callCompactionOnStorage(f)
		err = nil
	}
	return err
}

func (dm *DMap) deleteFromPreviousOwners(name, key string, owners []discovery.Member) error {
	// Traverse in reverse order. Except from the latest host, this one.
	for i := len(owners) - 2; i >= 0; i-- {
		owner := owners[i]
		req := protocol.NewDMapMessage(protocol.OpDeletePrev)
		req.SetDMap(name)
		req.SetKey(key)
		_, err := dm.s.client.RequestTo2(owner.String(), req)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dm *DMap) deleteFromBackup(hkey uint64, name, key string) error {
	owners := dm.s.backup.PartitionOwnersByHKey(hkey)
	var g errgroup.Group
	for _, owner := range owners {
		mem := owner
		g.Go(func() error {
			// TODO: Add retry with backoff
			req := protocol.NewDMapMessage(protocol.OpDeleteBackup)
			req.SetDMap(name)
			req.SetKey(key)
			_, err := dm.s.client.RequestTo2(mem.String(), req)
			if err != nil {
				dm.s.log.V(3).Printf("[ERROR] Failed to delete backup key/value on %s: %s", name, err)
			}
			return err
		})
	}
	return g.Wait()
}

func (dm *DMap) deleteOnFragment(hkey uint64, name, key string) error {
	owners := dm.s.primary.PartitionOwnersByHKey(hkey)
	if len(owners) == 0 {
		panic("partition owners list cannot be empty")
	}

	err := dm.deleteFromPreviousOwners(name, key, owners)
	if err != nil {
		return err
	}

	if dm.s.config.ReplicaCount != 0 {
		err := dm.deleteFromBackup(hkey, name, key)
		if err != nil {
			return err
		}
	}

	f, err := dm.getFragment(hkey, partitions.PRIMARY)
	if err != nil {
		return err
	}
	f.Lock()
	defer f.Unlock()

	err = f.storage.Delete(hkey)
	if err == storage.ErrFragmented {
		dm.s.wg.Add(1)
		go dm.s.callCompactionOnStorage(f)
		err = nil
	}

	// Delete it from access log if everything is ok.
	// If we delete the hkey when err is not nil, LRU/MaxIdleDuration may not work properly.
	if err == nil {
		dm.deleteAccessLog(hkey, f)
	}
	return err
}

func (dm *DMap) deleteKey(name, key string) error {
	hkey := partitions.HKey(name, key)
	member := dm.s.primary.PartitionByHKey(hkey).Owner()
	if !member.CompareByName(dm.s.rt.This()) {
		req := protocol.NewDMapMessage(protocol.OpDelete)
		req.SetDMap(name)
		req.SetKey(key)
		_, err := dm.s.client.RequestTo2(member.String(), req)
		return err
	}
	return dm.deleteOnFragment(hkey, name, key)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist. It's thread-safe.
// It is safe to modify the contents of the argument after Delete returns.
func (dm *DMap) Delete(key string) error {
	return dm.deleteKey(dm.name, key)
}
