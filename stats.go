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

func toMember(member discovery.Member) stats.Member {
	return stats.Member{
		Name:      member.Name,
		ID:        member.ID,
		Birthdate: member.Birthdate,
	}
}

func toMembers(members []discovery.Member) []stats.Member {
	var _stats []stats.Member
	for _, m := range members {
		_stats = append(_stats, toMember(m))
	}
	return _stats
}

func (db *Olric) collectPartitionStats(partID uint64, part *partition) stats.Partition {
	p := stats.Partition{
		Backups: toMembers(db.backups[partID].loadOwners()),
		Length:  part.length(),
		DMaps:   make(map[string]stats.DMap),
	}
	owners := part.loadOwners()
	if len(owners) > 0 {
		p.PreviousOwners = toMembers(owners[:len(owners)-1])
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

func (db *Olric) stats() stats.Stats {
	mem := &runtime.MemStats{}
	runtime.ReadMemStats(mem)
	s := stats.Stats{
		Cmdline:            os.Args,
		ReleaseVersion:     ReleaseVersion,
		ClusterCoordinator: toMember(db.discovery.GetCoordinator()),
		Runtime: stats.Runtime{
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			Version:      runtime.Version(),
			NumCPU:       runtime.NumCPU(),
			NumGoroutine: runtime.NumGoroutine(),
			MemStats:     *mem,
		},
		Member:         toMember(db.this),
		Partitions:     make(map[uint64]stats.Partition),
		Backups:        make(map[uint64]stats.Partition),
		ClusterMembers: make(map[uint64]stats.Member),
	}

	db.members.mtx.RLock()
	for id, member := range db.members.m {
		// List of bootstrapped cluster members
		s.ClusterMembers[id] = toMember(member)
	}
	db.members.mtx.RUnlock()

	routingMtx.RLock()
	for partID, part := range db.partitions {
		if db.checkOwnership(part) {
			s.Partitions[partID] = db.collectPartitionStats(partID, part)
		}
	}

	for partID, part := range db.backups {
		if db.checkOwnership(part) {
			s.Backups[partID] = db.collectPartitionStats(partID, part)
		}
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

// Stats collects and returns Golang runtime metrics and partition statistics.
// All data is belong to the current node. See stats.Stats for more information.
func (db *Olric) Stats() (stats.Stats, error) {
	if err := db.checkOperationStatus(); err != nil {
		return stats.Stats{}, err
	}
	return db.stats(), nil
}
