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
	"fmt"
	"io"
	"log"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/hashicorp/memberlist"
)

const (
	// SyncBackupMode enables sync backup mode which means that the caller is blocked
	// until write/delete operation is applied by backup owners.
	// The default mode is SyncBackupMode
	SyncBackupMode = 0

	// AsyncBackupMode enables async backup mode which means that write/delete operations
	// are done in a background task.
	AsyncBackupMode = 1
)

const (
	// DefaultPartitionCount determines default partition count in the cluster.
	DefaultPartitionCount = 271

	// DefaultLoadFactor is used by the consistent hashing function. Keep it small.
	DefaultLoadFactor = 1.25

	// DefaultLogLevel determines the log level without extra configuration. It's DEBUG.
	DefaultLogLevel = "DEBUG"
)

// OpMode is the type for operation modes.
type OpMode uint8

const (
	// OpInMemory indicates pure in-memory operation mode. In-memory data structures
	// are not durable at that mode.
	OpInMemory OpMode = OpMode(iota)

	// OpInMemoryWithSnapshot indicates in-memory operation mode with snapshot support.
	// The in-memory data is durable at that mode.
	OpInMemoryWithSnapshot
)

// Config is the configuration for creating a Olric instance.
type Config struct {
	LogLevel string
	// Name of this node in the cluster. This must be unique in the cluster. If this is not set,
	// Olric will set it to the hostname of the running machine. Example: node1.my-cluster.net
	//
	// Name is also used by the TCP server as Addr. It should be an IP adress or domain name of the server.
	Name string

	OperationMode OpMode

	KeepAlivePeriod time.Duration

	DialTimeout time.Duration

	// The list of host:port which are used by memberlist for discovery. Don't confuse it with Name.
	Peers []string

	// PartitionCount is 271, by default.
	PartitionCount uint64

	// BackupCount is 0, by default.
	BackupCount int

	// Default value is SyncBackupMode.
	BackupMode int

	// LoadFactor is used by consistent hashing function. It determines the maximum load
	// for a server in the cluster. Keep it small.
	LoadFactor float64

	MaxValueSize int

	// Default hasher is github.com/cespare/xxhash. You may want to use a different
	// hasher which implements Hasher interface.
	Hasher Hasher

	// Default Serializer implementation uses gob for encoding/decoding.
	Serializer Serializer

	// TLS certificate file for TCP server. If it's empty, TLS is disabled.
	CertFile string

	// TLS key file for TCP server. If it's empty, TLS is disabled.
	KeyFile string

	// LogOutput is the writer where logs should be sent. If this is not
	// set, logging will go to stderr by default. You cannot specify both LogOutput
	// and Logger at the same time.
	LogOutput io.Writer

	// Logger is a custom logger which you provide. If Logger is set, it will use
	// this for the internal logger. If Logger is not set, it will fall back to the
	// behavior for using LogOutput. You cannot specify both LogOutput and Logger
	// at the same time.
	Logger *log.Logger

	SnapshotInterval time.Duration
	GCInterval       time.Duration
	GCDiscardRatio   float64
	BadgerOptions    *badger.Options

	// MemberlistConfig is the memberlist configuration that Olric will
	// use to do the underlying membership management and gossip. Some
	// fields in the MemberlistConfig will be overwritten by Olric no
	// matter what:
	//
	//   * Name - This will always be set to the same as the NodeName
	//     in this configuration.
	//
	//   * Events - Olric uses a custom event delegate.
	//
	//   * Delegate - Olric uses a custom delegate.
	//
	// You have to use NewMemberlistConfig to create a new one.
	// Then, you may need to modify it to tune for your environment.
	MemberlistConfig *memberlist.Config
}

// NewMemberlistConfig returns a new memberlist.Config from vendored version of that package.
// It takes an env parameter: local, lan and wan.
//
// local:
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration that
// is optimized for a local loopback environments. The default configuration is still very conservative
// and errs on the side of caution.
//
// lan:
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses the hostname
// as the node name, and otherwise sets very conservative values that are sane for most LAN environments.
// The default configuration errs on the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good starting point when getting started with memberlist.
//
// wan:
// DefaultWANConfig works like DefaultConfig, however it returns a configuration that is optimized for most WAN environments.
// The default configuration is still very conservative and errs on the side of caution.
func NewMemberlistConfig(env string) (*memberlist.Config, error) {
	switch {
	case env == "local":
		return memberlist.DefaultLocalConfig(), nil
	case env == "lan":
		return memberlist.DefaultLANConfig(), nil
	case env == "wan":
		return memberlist.DefaultWANConfig(), nil
	}
	return nil, fmt.Errorf("unknown env: %s", env)
}
