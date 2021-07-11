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

/*Package stats exposes internal data structures for Stat command*/
package stats

import "runtime"

type (
	// PartitionID denotes ID of a partition in the cluster.
	PartitionID uint64

	// MemberID denotes ID of a member in the cluster.
	MemberID uint64
)

// SlabInfo denotes memory usage of the storage engine(a hash indexed, append only byte slice).
type SlabInfo struct {
	// Total allocated space by the append-only byte slice.
	Allocated int

	// Total inuse memory space in the append-only byte slice.
	Inuse int

	// Total garbage(deleted key/value pairs) space in the append-only byte slice.
	Garbage int
}

// DMap denotes a distributed map instance on the cluster.
type DMap struct {
	// Number of keys in the DMap.
	Length int

	// Statistics about memory representation of a DMap.
	SlabInfo SlabInfo

	// Number of tables in a storage instance.
	NumTables int
}

// Partition denotes a partition and its metadata in the cluster.
type Partition struct {
	// PreviousOwners is a list of members whose still owns some fragments.
	PreviousOwners []Member

	// Backups is a list of members whose holds replicas of this partition.
	Backups []Member

	// Total number of entries in the partition.
	Length int

	// DMaps is a map that contains statistics of DMaps in this partition.
	DMaps map[string]DMap
}

// Runtime exposes memory stats and various metrics from Go runtime.
type Runtime struct {
	// GOOS is the running program's operating system target
	GOOS string

	// GOARCH is the running program's architecture target
	GOARCH string

	// Version returns the Go tree's version string.
	Version string

	// NumCPU returns the number of logical CPUs usable by the current process.
	NumCPU int

	// NumGoroutine returns the number of goroutines that currently exist.
	NumGoroutine int

	// MemStats records statistics about the memory allocator.
	MemStats runtime.MemStats
}

// Member denotes a cluster member.
type Member struct {
	// Name is name of the node in the cluster.
	Name string

	// ID is the unique identifier of this node in the cluster. It's derived
	// from Name and Birthdate.
	ID uint64

	// Birthdate is UNIX time in nanoseconds.
	Birthdate int64
}

// String returns the member name.
func (m Member) String() string {
	return m.Name
}

// Network holds network statistics.
type Network struct {
	// ConnectionsTotal is total number of connections opened since the server started running.
	ConnectionsTotal int64

	// CurrentConnections is current number of open connections.
	CurrentConnections int64

	// WrittenBytesTotal is total number of bytes sent by this server to network.
	WrittenBytesTotal int64

	// ReadBytesTotal is total number of bytes read by this server from network.
	ReadBytesTotal int64

	// CommandsTotal is total number of all requests (get, put, etc.).
	CommandsTotal int64
}

// DMaps holds global DMap statistics.
type DMaps struct {
	// EntriesTotal is the total number of entries(including replicas) stored during the life of this instance.
	EntriesTotal int64

	// DeleteHits is the number of deletion reqs resulting in an item being removed.
	DeleteHits int64

	// DeleteMisses is the number of deletions reqs for missing keys
	DeleteMisses int64

	// GetMisses is the number of entries that have been requested and not found
	GetMisses int64

	// GetHits is the number of entries that have been requested and found present
	GetHits int64

	// EvictedTotal is the number of entries removed from cache to free memory for new entries.
	EvictedTotal int64
}

// DTopics holds global DTopic statistics.
type DTopics struct {
	// PublishedTotal is the total number of published messages to DTopics during the life of this instance.
	PublishedTotal int64

	// CurrentListeners is the current number of DTopic listeners of DTopics.
	CurrentListeners int64

	// ListenersTotal is the total number of registered DTopic listeners during the life of this instance.
	ListenersTotal int64
}

// Stats is a struct that exposes statistics about the current state of a member.
type Stats struct {
	// Cmdline holds the command-line arguments, starting with the program name.
	Cmdline []string

	// ReleaseVersion is the current Olric version
	ReleaseVersion string

	// UptimeSeconds is number of seconds since the server started.
	UptimeSeconds int64

	// Stats from Golang runtime
	Runtime *Runtime

	// ClusterCoordinator is the current cluster coordinator.
	ClusterCoordinator Member

	// Member denotes the current member.
	Member Member

	// Partitions is a map that contains partition statistics.
	Partitions map[PartitionID]Partition

	// Backups is a map that contains backup partition statistics.
	Backups map[PartitionID]Partition

	// ClusterMembers is a map that contains bootstrapped cluster members
	ClusterMembers map[MemberID]Member

	// Network holds network statistics.
	Network Network

	// DMaps holds global DMap statistics.
	DMaps DMaps

	// DTopics holds global DTopic statistics.
	DTopics DTopics
}
