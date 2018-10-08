// Copyright 2018 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"net"
	"strconv"
	"time"

	"github.com/buraksezer/olricdb"
	memlist "github.com/hashicorp/memberlist"
)

// newMemberlistConf creates a new *memberlist.Config by parsing olricd.toml.
func newMemberlistConf(c *Config) (*memlist.Config, error) {
	bindAddr, sport, err := net.SplitHostPort(c.Memberlist.Addr)
	if err != nil {
		return nil, err
	}
	bindPort, err := strconv.Atoi(sport)
	if err != nil {
		return nil, err
	}

	mc, err := olricdb.NewMemberlistConfig(c.Memberlist.Environment)
	if err != nil {
		return nil, err
	}

	mc.BindAddr = bindAddr
	mc.BindPort = bindPort
	mc.EnableCompression = c.Memberlist.EnableCompression
	if len(c.Memberlist.TCPTimeout) != 0 {
		mc.TCPTimeout, err = time.ParseDuration(c.Memberlist.TCPTimeout)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.IndirectChecks != 0 {
		mc.IndirectChecks = c.Memberlist.IndirectChecks
	}

	if c.Memberlist.RetransmitMult != 0 {
		mc.RetransmitMult = c.Memberlist.RetransmitMult
	}

	if c.Memberlist.SuspicionMult != 0 {
		mc.SuspicionMult = c.Memberlist.SuspicionMult
	}

	if c.Memberlist.PushPullInterval != "" {
		mc.PushPullInterval, err = time.ParseDuration(c.Memberlist.PushPullInterval)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.ProbeTimeout != "" {
		mc.ProbeTimeout, err = time.ParseDuration(c.Memberlist.ProbeTimeout)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.ProbeInterval != "" {
		mc.ProbeInterval, err = time.ParseDuration(c.Memberlist.ProbeInterval)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.GossipInterval != "" {
		mc.GossipInterval, err = time.ParseDuration(c.Memberlist.GossipInterval)
		if err != nil {
			return nil, err
		}
	}

	if c.Memberlist.GossipToTheDeadTime != "" {
		mc.GossipToTheDeadTime, err = time.ParseDuration(c.Memberlist.GossipToTheDeadTime)
		if err != nil {
			return nil, err
		}
	}
	return mc, nil
}
