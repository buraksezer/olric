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

package olric

import (
	"os"
	"runtime"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
	"github.com/buraksezer/olric/internal/dmap"
	"github.com/buraksezer/olric/internal/dtopic"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
	"github.com/buraksezer/olric/pkg/neterrors"
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

func (db *Olric) collectPartitionMetrics(partID uint64, part *partitions.Partition) stats.Partition {
	owners := part.Owners()
	p := stats.Partition{
		Backups: toMembers(db.backup.PartitionOwnersByID(partID)),
		Length:  part.Length(),
		DMaps:   make(map[string]stats.DMap),
	}
	if len(owners) > 0 {
		p.PreviousOwners = toMembers(owners[:len(owners)-1])
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

func (db *Olric) checkPartitionOwnership(part *partitions.Partition) bool {
	owners := part.Owners()
	for _, owner := range owners {
		if owner.CompareByID(db.rt.This()) {
			return true
		}
	}
	return false
}

func (db *Olric) stats(cfg statsConfig) stats.Stats {
	s := stats.Stats{
		Cmdline:            os.Args,
		ReleaseVersion:     ReleaseVersion,
		UptimeSeconds:      discovery.UptimeSeconds.Read(),
		ClusterCoordinator: toMember(db.rt.Discovery().GetCoordinator()),
		Member:             toMember(db.rt.This()),
		Partitions:         make(map[stats.PartitionID]stats.Partition),
		Backups:            make(map[stats.PartitionID]stats.Partition),
		ClusterMembers:     make(map[stats.MemberID]stats.Member),
		Network: stats.Network{
			ConnectionsTotal:   transport.ConnectionsTotal.Read(),
			CurrentConnections: transport.CurrentConnections.Read(),
			WrittenBytesTotal:  transport.WrittenBytesTotal.Read(),
			ReadBytesTotal:     transport.ReadBytesTotal.Read(),
			CommandsTotal:      transport.CommandsTotal.Read(),
		},
		DMaps: stats.DMaps{
			EntriesTotal: dmap.EntriesTotal.Read(),
			DeleteHits:   dmap.DeleteHits.Read(),
			DeleteMisses: dmap.DeleteMisses.Read(),
			GetMisses:    dmap.GetMisses.Read(),
			GetHits:      dmap.GetHits.Read(),
			EvictedTotal: dmap.EvictedTotal.Read(),
		},
		DTopics: stats.DTopics{
			PublishedTotal:   dtopic.PublishedTotal.Read(),
			CurrentListeners: dtopic.CurrentListeners.Read(),
			ListenersTotal:   dtopic.ListenersTotal.Read(),
		},
	}

	if cfg.CollectRuntime {
		s.Runtime = &stats.Runtime{
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			Version:      runtime.Version(),
			NumCPU:       runtime.NumCPU(),
			NumGoroutine: runtime.NumGoroutine(),
		}
		runtime.ReadMemStats(&s.Runtime.MemStats)
	}

	db.rt.RLock()
	defer db.rt.RUnlock()

	db.rt.Members().Range(func(id uint64, member discovery.Member) bool {
		s.ClusterMembers[stats.MemberID(id)] = toMember(member)
		return true
	})

	for partID := uint64(0); partID < db.config.PartitionCount; partID++ {
		primary := db.primary.PartitionByID(partID)
		if db.checkPartitionOwnership(primary) {
			s.Partitions[stats.PartitionID(partID)] = db.collectPartitionMetrics(partID, primary)
		}
		backup := db.backup.PartitionByID(partID)
		if db.checkPartitionOwnership(backup) {
			s.Backups[stats.PartitionID(partID)] = db.collectPartitionMetrics(partID, backup)
		}
	}

	return s
}

func (db *Olric) statsOperation(w, r protocol.EncodeDecoder) {
	extra := r.Extra().(protocol.StatsExtra)
	cfg := statsConfig{
		CollectRuntime: extra.CollectRuntime,
	}
	s := db.stats(cfg)
	value, err := msgpack.Marshal(s)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

type statsConfig struct {
	CollectRuntime bool
}

type StatsOption func(*statsConfig)

func CollectRuntime() StatsOption {
	return func(cfg *statsConfig) {
		cfg.CollectRuntime = true
	}
}

// Stats exposes some useful metrics to monitor an Olric node.
func (db *Olric) Stats(options ...StatsOption) (stats.Stats, error) {
	if err := db.isOperable(); err != nil {
		// this node is not bootstrapped yet.
		return stats.Stats{}, err
	}
	var cfg statsConfig
	for _, opt := range options {
		opt(&cfg)
	}
	return db.stats(cfg), nil
}
