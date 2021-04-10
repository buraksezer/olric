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

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/stats"
	"github.com/vmihailenco/msgpack"
)

func (db *Olric) stats() stats.Stats {
	mem := &runtime.MemStats{}
	runtime.ReadMemStats(mem)
	s := stats.Stats{
		Cmdline:            os.Args,
		ReleaseVersion:     ReleaseVersion,
		ClusterCoordinator: db.rt.Discovery().GetCoordinator(),
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

	db.rt.RLock()
	db.rt.Members().Range(func(id uint64, member discovery.Member) bool {
		m := &member
		s.ClusterMembers[id] = *m
		return true
	})
	db.rt.RUnlock()

	collect := func(partID uint64, part *partitions.Partition) stats.Partition {
		owners := part.Owners()
		p := stats.Partition{
			Backups: db.backup.PartitionOwnersById(partID),
			Length:  part.Length(),
			DMaps:   make(map[string]stats.DMap),
		}
		if part.Kind() == partitions.PRIMARY {
			p.Owner = part.Owner()
		}
		if len(owners) > 0 {
			p.PreviousOwners = owners[:len(owners)-1]
		}
		part.Map().Range(func(name, item interface{}) bool {
			f := item.(partitions.Fragment)
			st := f.Stats()
			tmp := stats.DMap{
				Length:    st.Length,
				NumTables: st.NumTables,
			}
			tmp.SlabInfo.Allocated = st.Allocated
			tmp.SlabInfo.Garbage = st.Garbage
			tmp.SlabInfo.Inuse = st.Inuse
			p.DMaps[name.(string)] = tmp
			return true
		})
		return p
	}

	db.rt.RLock()
	defer db.rt.RUnlock()
	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		part := db.primary.PartitionById(partID)
		s.Partitions[partID] = collect(partID, part)
		s.Backups[partID] = collect(partID, part)
	}

	return s
}

func (db *Olric) statsOperation(w, _ protocol.EncodeDecoder) {
	s := db.stats()
	value, err := msgpack.Marshal(s)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

// Stats exposes some useful metrics to monitor an Olric node.
func (db *Olric) Stats() (stats.Stats, error) {
	if err := db.isOperable(); err != nil {
		return stats.Stats{}, err
	}
	return db.stats(), nil
}
