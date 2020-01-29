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
	"strings"
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

	// DefaultRequestTimeout denotes default timeout value for a request.
	DefaultRequestTimeout = 10 * time.Second

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

// EvictionPolicy denotes eviction policy. Currently: LRU or NONE.
type EvictionPolicy string

// note on DMapCacheConfig and CacheConfig:
// golang doesn't provide the typical notion of inheritance.
// because of that I preferred to define the types explicitly.

// DMapCacheConfig denotes cache configuration for a particular DMap.
type DMapCacheConfig struct {
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the DMap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a DMap instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10 nodes with
	// MaxKeys=100000, your key count in the cluster should be around MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node. So if you have 10 nodes with
	// MaxInuse=100M (it has to be in bytes), amount of in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the aproximate LRU implementation.
	// Lower values are better for high performance. It's 5 by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy
}

// CacheConfig denotes a global cache configuration for DMaps. You can still overwrite it by setting a
// DMapCacheConfig for a particular DMap. Don't set this if you use Olric as an ordinary key/value store.
type CacheConfig struct {
	// NumEvictionWorkers denotes the number of goroutines that's used to find keys for eviction.
	NumEvictionWorkers int64
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the DMap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a DMap instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10 nodes with
	// MaxKeys=100000, max key count in the cluster should around MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node. So if you have 10 nodes with
	// MaxInuse=100M (it has to be in bytes), max amount of in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the aproximate LRU implementation.
	// Lower values are better for high performance. It's 5 by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy

	// DMapConfigs is useful to set custom cache config per DMap instance.
	DMapConfigs map[string]DMapCacheConfig
}

// Config is the configuration to create a Olric instance.
type Config struct {
	// LogVerbosity denotes the level of message verbosity. The default value is 3. Valid values are between 1 to 6.
	LogVerbosity int32

	// Default LogLevel is DEBUG. Valid ones: "DEBUG", "WARN", "ERROR", "INFO"
	LogLevel string

	// Name of this node in the cluster. This must be unique in the cluster. If this is not set,
	// Olric will set it to the hostname of the running machine. Example: node1.my-cluster.net
	//
	// Name is also used by the TCP server as Addr. It should be an IP address or domain name of the server.
	Name string

	// KeepAlivePeriod denotes whether the operating system should send keep-alive messages on the connection.
	KeepAlivePeriod time.Duration

	// Timeout for TCP dial.
	//
	// The timeout includes name resolution, if required. When using TCP, and the host in the address parameter
	// resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
	// given an appropriate fraction of the time to connect.
	DialTimeout time.Duration

	RequestTimeout time.Duration

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
	e := strings.ToLower(env)
	switch e {
	case "local":
		return memberlist.DefaultLocalConfig(), nil
	case "lan":
		return memberlist.DefaultLANConfig(), nil
	case "wan":
		return memberlist.DefaultWANConfig(), nil
	}
	return nil, fmt.Errorf("unknown env: %s", env)
}

func (c *Config) validateMemberlistConfig() error {
	var result error
	if len(c.MemberlistConfig.AdvertiseAddr) != 0 {
		if ip := net.ParseIP(c.MemberlistConfig.AdvertiseAddr); ip == nil {
			result = multierror.Append(result,
				fmt.Errorf("memberlist: AdvertiseAddr has to be a valid IPv4 or IPv6 address"))
		}
	}
	if len(c.MemberlistConfig.BindAddr) == 0 {
		result = multierror.Append(result,
			fmt.Errorf("memberlist: BindAddr cannot be an empty string"))
	}
	return result
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
	if c.Name == "" {
		name, err := os.Hostname()
		if err != nil {
			return err
		}
		c.Name = name + ":0"
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
		c.MemberlistConfig = memberlist.DefaultLocalConfig()
	}
	if c.RequestTimeout == 0*time.Second {
		c.RequestTimeout = DefaultRequestTimeout
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
	return nil
}
