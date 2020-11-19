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

package config

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/buraksezer/olric/hasher"
	"github.com/buraksezer/olric/serializer"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/memberlist"
)

const (
	// SyncReplicationMode enables sync replication mode which means that the caller is blocked
	// until write/delete operation is applied by replica owners. The default mode is SyncReplicationMode
	SyncReplicationMode = 0

	// AsyncReplicationMode enables async replication mode which means that write/delete operations
	// are done in a background task.
	AsyncReplicationMode = 1
)

const (
	// DefaultPort is for Olric
	DefaultPort = 3320

	// DefaultDiscoveryPort is for memberlist
	DefaultDiscoveryPort = 3322

	// DefaultPartitionCount denotes default partition count in the cluster.
	DefaultPartitionCount = 271

	// DefaultLoadFactor is used by the consistent hashing function. Keep it small.
	DefaultLoadFactor = 1.25

	// DefaultLogLevel determines the log level without extra configuration. It's DEBUG.
	DefaultLogLevel = "DEBUG"

	// DefaultLogVerbosity denotes default log verbosity level.
	//
	// * flog.V(1) - Generally useful for this to ALWAYS be visible to an operator
	//   * Programmer errors
	//   * Logging extra info about a panic
	//   * CLI argument handling
	// * flog.V(2) - A reasonable default log level if you don't want verbosity.
	//   * Information about config (listening on X, watching Y)
	//   * Errors that repeat frequently that relate to conditions that can be corrected (pod detected as unhealthy)
	// * flog.V(3) - Useful steady state information about the service and important log messages that may correlate to
	//   significant changes in the system.  This is the recommended default log level for most systems.
	//   * Logging HTTP requests and their exit code
	//   * System state changing (killing pod)
	//   * Controller state change events (starting pods)
	//   * Scheduler log messages
	// * flog.V(4) - Extended information about changes
	//   * More info about system state changes
	// * flog.V(5) - Debug level verbosity
	//   * Logging in particularly thorny parts of code where you may want to come back later and check it
	// * flog.V(6) - Trace level verbosity
	//   * Context to understand the steps leading up to errors and warnings
	//   * More information for troubleshooting reported issues
	DefaultLogVerbosity = 3

	// MinimumReplicaCount denotes default and minimum replica count in an Olric cluster.
	MinimumReplicaCount = 1

	// DefaultBootstrapTimeout denotes default timeout value to check bootstrapping status.
	DefaultBootstrapTimeout = 10 * time.Second

	// DefaultJoinRetryInterval denotes a time gap between sequential join attempts.
	DefaultJoinRetryInterval = time.Second

	// DefaultMaxJoinAttempts denotes a maximum number of failed join attempts
	// before forming a standalone cluster.
	DefaultMaxJoinAttempts = 10

	// MinimumMemberCountQuorum denotes minimum required count of members to form a cluster.
	MinimumMemberCountQuorum = 1

	// DefaultTableSize is 1MB if you don't set your own value.
	DefaultTableSize = 1 << 20

	DefaultLRUSamples int = 5

	// Assign this as EvictionPolicy in order to enable LRU eviction algorithm.
	LRUEviction EvictionPolicy = "LRU"
)

// Config is the configuration to create a Olric instance.
type Config struct {
	// Interface denotes a binding interface. It can be used instead of BindAddr if the interface is known but not the address.
	// If both are provided, then Olric verifies that the interface has the bind address that is provided.
	Interface string

	// LogVerbosity denotes the level of message verbosity. The default value is 3. Valid values are between 1 to 6.
	LogVerbosity int32

	// Default LogLevel is DEBUG. Valid ones: "DEBUG", "WARN", "ERROR", "INFO"
	LogLevel string

	// BindAddr denotes the address that Olric will bind to for communication with other Olric nodes.
	BindAddr string

	// BindPort denotes the address that Olric will bind to for communication with other Olric nodes.
	BindPort int

	Client *Client

	// KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.
	KeepAlivePeriod time.Duration

	// Timeout for bootstrap control
	//
	// An Olric node checks operation status before taking any action for the cluster events, responding incoming requests
	// and running API functions. Bootstrapping status is one of the most important checkpoints for an "operable" Olric node.
	// BootstrapTimeout sets a deadline to check bootstrapping status without blocking indefinitely.
	BootstrapTimeout time.Duration

	// The list of host:port which are used by memberlist for discovery. Don't confuse it with Name.
	Peers []string

	// PartitionCount is 271, by default.
	PartitionCount uint64

	// ReplicaCount is 1, by default.
	ReplicaCount int

	// Minimum number of successful reads to return a response for a read request.
	ReadQuorum int

	// Minimum number of successful writes to return a response for a write request.
	WriteQuorum int

	// Minimum number of members to form a cluster and run any query on the cluster.
	MemberCountQuorum int32

	// Switch to control read-repair algorithm which helps to reduce entropy.
	ReadRepair bool

	// Default value is SyncReplicationMode.
	ReplicationMode int

	// LoadFactor is used by consistent hashing function. It determines the maximum load
	// for a server in the cluster. Keep it small.
	LoadFactor float64

	// Default hasher is github.com/cespare/xxhash
	Hasher hasher.Hasher

	// Default Serializer implementation uses gob for encoding/decoding.
	Serializer serializer.Serializer

	// LogOutput is the writer where logs should be sent. If this is not
	// set, logging will go to stderr by default. You cannot specify both LogOutput
	// and Logger at the same time.
	LogOutput io.Writer

	// Logger is a custom logger which you provide. If Logger is set, it will use
	// this for the internal logger. If Logger is not set, it will fall back to the
	// behavior for using LogOutput. You cannot specify both LogOutput and Logger
	// at the same time.
	Logger *log.Logger

	Cache *CacheConfig

	// Minimum size(in-bytes) for append-only file
	TableSize int

	JoinRetryInterval time.Duration
	MaxJoinAttempts   int

	// Callback function. Olric calls this after
	// the server is ready to accept new connections.
	Started func()

	ServiceDiscovery map[string]interface{}

	// Interface denotes a binding interface. It can be used instead of memberlist.Loader.BindAddr if the interface is
	// known but not the address. If both are provided, then Olric verifies that the interface has the bind address that
	// is provided.
	MemberlistInterface string

	// MemberlistConfig is the memberlist configuration that Olric will
	// use to do the underlying membership management and gossip. Some
	// fields in the MemberlistConfig will be overwritten by Olric no
	// matter what:
	//
	//   * Name - This will always be set to the same as the NodeName
	//     in this configuration.
	//
	//   * ClusterEvents - Olric uses a custom event delegate.
	//
	//   * Delegate - Olric uses a custom delegate.
	//
	// You have to use NewMemberlistConfig to create a new one.
	// Then, you may need to modify it to tune for your environment.
	MemberlistConfig *memberlist.Config
}

// Validate validates the given configuration.
func (c *Config) Validate() error {
	var result error
	if c.ReplicaCount < MinimumReplicaCount {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReplicaCount smaller than MinimumReplicaCount"))
	}

	if c.ReadQuorum <= 0 {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReadQuorum less than or equal to zero"))
	}
	if c.ReplicaCount < c.ReadQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify ReadQuorum greater than ReplicaCount"))
	}

	if c.WriteQuorum <= 0 {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify WriteQuorum less than or equal to zero"))
	}
	if c.ReplicaCount < c.WriteQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify WriteQuorum greater than ReplicaCount"))
	}

	if err := c.validateMemberlistConfig(); err != nil {
		result = multierror.Append(result, err)
	}

	if c.MemberCountQuorum < MinimumMemberCountQuorum {
		result = multierror.Append(result,
			fmt.Errorf("cannot specify MemberCountQuorum "+
				"smaller than MinimumMemberCountQuorum"))
	}

	if c.BindAddr == "" {
		result = multierror.Append(result, fmt.Errorf("BindAddr cannot be empty"))
	}

	if c.BindPort == 0 {
		result = multierror.Append(result, fmt.Errorf("BindPort cannot be empty or zero"))
	}

	return result
}

// Sanitize sanitizes the given configuration.
// It returns an error if there is something very bad in the configuration.
func (c *Config) Sanitize() error {
	if c.Logger == nil {
		if c.LogOutput == nil {
			c.LogOutput = os.Stderr
		}
		if c.LogLevel == "" {
			c.LogLevel = DefaultLogLevel
		}
		c.Logger = log.New(c.LogOutput, "", log.LstdFlags)
	}

	if c.LogVerbosity <= 0 {
		c.LogVerbosity = DefaultLogVerbosity
	}

	if c.Hasher == nil {
		c.Hasher = hasher.NewDefaultHasher()
	}
	if c.Serializer == nil {
		c.Serializer = serializer.NewGobSerializer()
	}

	if c.BindAddr == "" {
		name, err := os.Hostname()
		if err != nil {
			return err
		}
		c.BindAddr = name
	}
	// We currently don't support ephemeral port selection. Because it needs improved flow
	// control in server initialization stage.
	if c.BindPort == 0 {
		c.BindPort = DefaultPort
	}

	if c.LoadFactor == 0 {
		c.LoadFactor = DefaultLoadFactor
	}
	if c.PartitionCount == 0 {
		c.PartitionCount = DefaultPartitionCount
	}
	if c.ReplicaCount == 0 {
		c.ReplicaCount = MinimumReplicaCount
	}
	if c.MemberlistConfig == nil {
		m := memberlist.DefaultLocalConfig()
		// hostname is assigned to memberlist.BindAddr
		// memberlist.Name is assigned by olric.New
		m.BindPort = DefaultDiscoveryPort
		m.AdvertisePort = DefaultDiscoveryPort
		c.MemberlistConfig = m
	}
	if c.BootstrapTimeout == 0*time.Second {
		c.BootstrapTimeout = DefaultBootstrapTimeout
	}
	if c.JoinRetryInterval == 0*time.Second {
		c.JoinRetryInterval = DefaultJoinRetryInterval
	}
	if c.MaxJoinAttempts == 0 {
		c.MaxJoinAttempts = DefaultMaxJoinAttempts
	}
	if c.TableSize == 0 {
		c.TableSize = DefaultTableSize
	}

	// Check peers. If Peers slice contains node's itself, return an error.
	port := strconv.Itoa(c.MemberlistConfig.BindPort)
	this := net.JoinHostPort(c.MemberlistConfig.BindAddr, port)
	for _, peer := range c.Peers {
		if this == peer {
			return fmt.Errorf("a node cannot be peer with itself")
		}
	}
	if c.Client == nil {
		c.Client = NewClient()
	} else {
		c.Client.Sanitize()
	}
	return nil
}

// DefaultConfig returns a Config with sane defaults.
// It takes an env parameter used by memberlist: local, lan and wan.
//
// local:
//
// DefaultLocalConfig works like DefaultConfig, however it returns a configuration that
// is optimized for a local loopback environments. The default configuration is still very conservative
// and errs on the side of caution.
//
// lan:
//
// DefaultLANConfig returns a sane set of configurations for Memberlist. It uses the hostname
// as the node name, and otherwise sets very conservative values that are sane for most LAN environments.
// The default configuration errs on the side of caution, choosing values that are optimized for higher convergence
// at the cost of higher bandwidth usage. Regardless, these values are a good starting point when getting started with memberlist.
//
// wan:
//
// DefaultWANConfig works like DefaultConfig, however it returns a configuration that is optimized for most WAN environments.
// The default configuration is still very conservative and errs on the side of caution.
func New(env string) *Config {
	c := &Config{
		BindAddr:          "0.0.0.0",
		BindPort:          DefaultPort,
		ReadRepair:        false,
		ReplicaCount:      1,
		WriteQuorum:       1,
		ReadQuorum:        1,
		MemberCountQuorum: 1,
		Peers:             []string{},
		Cache:             &CacheConfig{},
	}
	if err := c.Sanitize(); err != nil {
		panic(fmt.Sprintf("unable to sanitize Olric config: %v", err))
	}
	m, err := NewMemberlistConfig(env)
	if err != nil {
		panic(fmt.Sprintf("unable to create a new memberlist config: %v", err))
	}
	// memberlist.Name will be assigned by olric.New
	m.BindPort = DefaultDiscoveryPort
	m.AdvertisePort = DefaultDiscoveryPort
	c.MemberlistConfig = m

	if err := c.Validate(); err != nil {
		panic(fmt.Sprintf("unable to validate Olric config: %v", err))
	}
	return c
}
