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

package olric

import (
	"os"
	"runtime"

	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/stats"
	"github.com/vmihailenco/msgpack"
)

func (db *Olric) stats() stats.Stats {
	mem := &runtime.MemStats{}
	runtime.ReadMemStats(mem)
	s := stats.Stats{
		Cmdline:            os.Args,
		ReleaseVersion:     ReleaseVersion,
		ClusterCoordinator: db.discovery.GetCoordinator(),
		Runtime: stats.Runtime{
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			Version:      runtime.Version(),
			NumCPU:       runtime.NumCPU(),
			NumGoroutine: runtime.NumGoroutine(),
			MemStats:     *mem,
		},
		Partitions:     make(map[uint64]stats.Partition),
		Backups:        make(map[uint64]stats.Partition),
		ClusterMembers: make(map[uint64]discovery.Member),
	}

	db.members.mtx.RLock()
	for id, member := range db.members.m {
		// List of bootstrapped cluster members
		s.ClusterMembers[id] = member
	}
	db.members.mtx.RUnlock()

	collect := func(partID uint64, part *partition) stats.Partition {
		owners := part.loadOwners()
		p := stats.Partition{
			Backups: db.backups[partID].loadOwners(),
			Length:  part.length(),
			DMaps:   make(map[string]stats.DMap),
		}
		if !part.backup {
			p.Owner = part.owner()
		}
		if len(owners) > 0 {
			p.PreviousOwners = owners[:len(owners)-1]
		}
		part.m.Range(func(name, dm interface{}) bool {
			dm.(*dmap).Lock()
			tmp := stats.DMap{
				Length:    dm.(*dmap).storage.Len(),
				NumTables: dm.(*dmap).storage.NumTables(),
				SlabInfo:  stats.SlabInfo(dm.(*dmap).storage.SlabInfo()),
			}
			p.DMaps[name.(string)] = tmp
			dm.(*dmap).Unlock()
			return true
		})
		return p
	}
	routingMtx.RLock()
	for partID, part := range db.partitions {
		s.Partitions[partID] = collect(partID, part)
	}

	for partID, part := range db.backups {
		s.Backups[partID] = collect(partID, part)
	}
	routingMtx.RUnlock()
	return s
}

func (db *Olric) statsOperation(w, _ protocol.EncodeDecoder) {
	s := db.stats()
	value, err := msgpack.Marshal(s)
	if err != nil {
		db.errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

// Stats exposes some useful metrics to monitor an Olric node.
func (db *Olric) Stats() (stats.Stats, error) {
	if err := db.checkOperationStatus(); err != nil {
		return stats.Stats{}, err
	}
	return db.stats(), nil
}
