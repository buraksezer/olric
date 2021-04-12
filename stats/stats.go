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

/*Package stats exposes internal data structures for Stat command*/
package stats

import "runtime"

// SlabInfo denotes memory usage of the storage engine(a hash indexed append only log file).
type SlabInfo struct {
	// Total allocated space by the append-only log files.
	Allocated int

	// Total inuse memory space in the append-only log files.
	Inuse int

	// Total garbage(deleted key/value pairs) space in the append-only log files.
	Garbage int
}

// DMap denotes a distributed map instance on the cluster.
type DMap struct {
	Name   string
	Length int

	// Statistics about memory representation of the dmap.
	SlabInfo SlabInfo

	// Number of tables in a storage instance.
	NumTables int
}

// Partition denotes a partition and its metadata in the cluster.
type Partition struct {
	PreviousOwners []Member
	Backups        []Member
	Length         int
	DMaps          map[string]DMap
}

// Runtime exposes memory stats and various metrics from Go runtime.
type Runtime struct {
	GOOS         string
	GOARCH       string
	Version      string
	NumCPU       int
	NumGoroutine int
	MemStats     runtime.MemStats
}

// Member denotes a cluster member
type Member struct {
	Name      string
	ID        uint64
	Birthdate int64
}

func (m Member) String() string {
	return m.Name
}

// Stats includes some metadata information about the cluster. The nodes add everything it knows about the cluster.
type Stats struct {
	Cmdline            []string
	ReleaseVersion     string
	Runtime            Runtime
	ClusterCoordinator Member
	Member             Member
	Partitions         map[uint64]Partition
	Backups            map[uint64]Partition
	ClusterMembers     map[uint64]Member
}
