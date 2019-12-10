// Copyright 2018-2019 Burak Sezer
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
	// DefaultPartitionCount determines default partition count in the cluster.
	DefaultPartitionCount = 271

	// DefaultLoadFactor is used by the consistent hashing function. Keep it small.
	DefaultLoadFactor = 1.25

	// DefaultLogLevel determines the log level without extra configuration. It's DEBUG.
	DefaultLogLevel = "DEBUG"

	DefaultLogVerbosity = 2

	// MinimumReplicaCount determines the default and minimum replica count in an Olric cluster.
	MinimumReplicaCount = 1

	DefaultRequestTimeout = 10 * time.Second

	DefaultJoinRetryInterval = time.Second

	DefaultMaxJoinAttempts = 10

	MinimumMemberCountQuorum = 1

	DefaultTableSize = 1 << 20
)

type EvictionPolicy string

const (
	DefaultLRUSamples int            = 5
	LRUEviction       EvictionPolicy = "LRU"
)

// note on DMapCacheConfig and CacheConfig:
// golang doesn't provide the typical notion of inheritance.
// because of that I preferred to define the types explicitly.

type DMapCacheConfig struct {
	MaxIdleDuration time.Duration
	TTLDuration     time.Duration
	MaxKeys         int
	MaxInuse        int
	LRUSamples      int
	EvictionPolicy  EvictionPolicy
}

type CacheConfig struct {
	NumEvictionWorkers int64
	MaxIdleDuration    time.Duration
	TTLDuration        time.Duration
	MaxKeys            int
	MaxInuse           int
	LRUSamples         int
	EvictionPolicy     EvictionPolicy
	DMapConfigs        map[string]DMapCacheConfig // For fine grained configuration.
}

// Config is the configuration to create a Olric instance.
type Config struct {
	LogVerbosity int32
	LogLevel     string
	// Name of this node in the cluster. This must be unique in the cluster. If this is not set,
	// Olric will set it to the hostname of the running machine. Example: node1.my-cluster.net
	//
	// Name is also used by the TCP server as Addr. It should be an IP address or domain name of the server.
	Name string

	KeepAlivePeriod time.Duration

	DialTimeout time.Duration

	RequestTimeout time.Duration

	// The list of host:port which are used by memberlist for discovery. Don't confuse it with Name.
	Peers []string

	// PartitionCount is 271, by default.
	PartitionCount uint64

	// ReplicaCount is 1, by default.
	ReplicaCount int

	ReadQuorum        int
	WriteQuorum       int
	MemberCountQuorum int32

	ReadRepair bool

	// Default value is SyncReplicationMode.
	ReplicationMode int

	// LoadFactor is used by consistent hashing function. It determines the maximum load
	// for a server in the cluster. Keep it small.
	LoadFactor float64

	// Default hasher is github.com/cespare/xxhash. You may want to use a different
	// hasher which implements Hasher interface.
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

	Cache     *CacheConfig
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
