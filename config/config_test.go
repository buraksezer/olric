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
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/buraksezer/olric/serializer"
)

var testConfig = `olricd:
  bindAddr: "0.0.0.0"
  bindPort: 3320
  serializer: "msgpack"
  keepAlivePeriod: "300s"
  bootstrapTimeout: "5s"
  partitionCount:  271
  replicaCount: 2
  writeQuorum: 1
  readQuorum: 1
  readRepair: false
  replicationMode: 0 # sync mode. for async, set 1
  tableSize: 1048576 # 1MB in bytes
  memberCountQuorum: 1

http:
  enabled: true
  bindAddr: "0.0.0.0"
  bindPort: 8080
  contentType: "application/json"
  readTimeout: "10s"
  writeTimeout: "10s"
  readHeaderTimeout: "10s"
  idleTimeout: "10s"
  maxHeaderBytes: 10

client:
  dialTimeout: "10s"
  readTimeout: "3s"
  writeTimeout: "3s"
  keepAlive: "15s"
  minConn: 1
  maxConn: 100

logging:
  verbosity: 6
  level: "DEBUG"
  output: "stderr"

memberlist:
  environment: "local"
  bindAddr: "0.0.0.0"
  bindPort: 3322
  enableCompression: false
  joinRetryInterval: "1s"
  maxJoinAttempts: 10
  peers:
    - "localhost:3325"

  advertiseAddr: ""
  advertisePort: 3322
  suspicionMaxTimeoutMult: 6
  disableTCPPings: false
  awarenessMaxMultiplier: 8
  gossipNodes: 3
  gossipVerifyIncoming: true
  gossipVerifyOutgoing: true
  dnsConfigPath: "/etc/resolv.conf"
  handoffQueueDepth: 1024
  udpBufferSize: 1400


cache:
  numEvictionWorkers: 1
  maxIdleDuration: ""
  ttlDuration: "100s"
  maxKeys: 100000
  maxInuse: 1000000
  lruSamples: 10
  evictionPolicy: "LRU"

dmaps:
  foobar:
    maxIdleDuration: "60s"
    ttlDuration: "300s"
    maxKeys: 500000
    lruSamples: 20
    evictionPolicy: "NONE"


serviceDiscovery:
  path: "/usr/lib/olric-consul-plugin.so"
  provider: "consul"
  address: "http://consul:8500"
  passingOnly: true
  replaceExistingChecks: true
  insecureSkipVerify: true
  payload: 'SAMPLE-PAYLOAD'`

func TestConfig(t *testing.T) {
	f, err := ioutil.TempFile("/tmp/", "olric-yaml-config-test")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	_, err = f.Write([]byte(testConfig))
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = f.Close()
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
	}()
	lc, err := Load(f.Name())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	c := New("local")
	c.BindAddr = "0.0.0.0"
	c.BindPort = 3320
	c.Serializer = serializer.NewMsgpackSerializer()
	c.KeepAlivePeriod = 300 * time.Second
	c.BootstrapTimeout = 5 * time.Second
	c.PartitionCount = 271
	c.ReplicaCount = 2
	c.WriteQuorum = 1
	c.ReadQuorum = 1
	c.ReadRepair = false
	c.ReplicationMode = SyncReplicationMode
	c.TableSize = 1048576
	c.MemberCountQuorum = 1

	c.Http.Enabled = true
	c.Http.BindAddr = "0.0.0.0"
	c.Http.BindPort = 8080
	c.Http.ContentType = "application/json"
	c.Http.ReadTimeout = 10 * time.Second
	c.Http.WriteTimeout = 10 * time.Second
	c.Http.ReadHeaderTimeout = 10 * time.Second
	c.Http.IdleTimeout = 10 * time.Second
	c.Http.MaxHeaderBytes = 10

	c.Client.DialTimeout = 10 * time.Second
	c.Client.ReadTimeout = 3 * time.Second
	c.Client.WriteTimeout = 3 * time.Second
	c.Client.KeepAlive = 15 * time.Second
	c.Client.MinConn = 1
	c.Client.MaxConn = 100

	c.LogVerbosity = 6
	c.LogLevel = "DEBUG"

	c.MemberlistConfig.BindAddr = "0.0.0.0"
	c.MemberlistConfig.BindPort = 3322
	c.MemberlistConfig.EnableCompression = false
	c.JoinRetryInterval = time.Second
	c.MaxJoinAttempts = 10
	c.Peers = []string{"localhost:3325"}
	c.MemberlistConfig.AdvertisePort = 3322
	c.MemberlistConfig.SuspicionMaxTimeoutMult = 6
	c.MemberlistConfig.DisableTcpPings = false
	c.MemberlistConfig.AwarenessMaxMultiplier = 8
	c.MemberlistConfig.GossipNodes = 3
	c.MemberlistConfig.GossipVerifyIncoming = true
	c.MemberlistConfig.GossipVerifyOutgoing = true
	c.MemberlistConfig.DNSConfigPath = "/etc/resolv.conf"
	c.MemberlistConfig.HandoffQueueDepth = 1024
	c.MemberlistConfig.UDPBufferSize = 1400

	c.Cache.NumEvictionWorkers = 1
	c.Cache.TTLDuration = 100 * time.Second
	c.Cache.MaxKeys = 100000
	c.Cache.MaxInuse = 1000000
	c.Cache.LRUSamples = 10
	c.Cache.EvictionPolicy = LRUEviction

	c.Cache.DMapConfigs = map[string]DMapCacheConfig{"foobar": {
		MaxIdleDuration: 60 * time.Second,
		TTLDuration:     300 * time.Second,
		MaxKeys:         500000,
		LRUSamples:      20,
		EvictionPolicy:  "NONE",
	}}

	c.ServiceDiscovery = make(map[string]interface{})
	c.ServiceDiscovery["path"] = "/usr/lib/olric-consul-plugin.so"
	c.ServiceDiscovery["provider"] = "consul"
	c.ServiceDiscovery["address"] = "http://consul:8500"
	c.ServiceDiscovery["passingOnly"] = true
	c.ServiceDiscovery["replaceExistingChecks"] = true
	c.ServiceDiscovery["insecureSkipVerify"] = true
	c.ServiceDiscovery["payload"] = "SAMPLE-PAYLOAD"

	// Disable the following fields. They include unexported fields, pointers and mutexes.
	c.LogOutput = nil
	lc.LogOutput = nil
	c.Logger = nil
	lc.Logger = nil

	if !reflect.DeepEqual(lc, c) {
		t.Fatalf("Expected true. Got: false")
	}
}
