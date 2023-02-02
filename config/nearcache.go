// Copyright 2018-2022 Burak Sezer
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
	"time"
)

type NearCache struct {
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the DMap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, Expire on it. Configuration of MaxIdleDuration feature varies by preferred
	// deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a
	// distributed map instance.
	TTLDuration time.Duration

	// MaxKeys denotes maximum key count on a particular node. So if you have 10
	// nodes with MaxKeys=100000, your key count in the cluster should be around
	// MaxKeys*10=1000000
	MaxKeys int

	// MaxInuse denotes maximum amount of in-use memory on a particular node.
	// So if you have 10 nodes with MaxInuse=100M (it has to be in bytes), amount
	// of in-use memory should be around MaxInuse*10=1G
	MaxInuse int

	// LRUSamples denotes amount of randomly selected key count by the approximate
	// LRU implementation. Lower values are better for high performance. It's
	// 5 by default.
	LRUSamples int

	// EvictionPolicy determines the eviction policy in use. It's NONE by default.
	// Set as LRU to enable LRU eviction policy.
	EvictionPolicy EvictionPolicy
}

// Sanitize sets default values to empty configuration variables, if it's possible.
func (nc *NearCache) Sanitize() error {
	if nc.LRUSamples <= 0 {
		nc.LRUSamples = DefaultLRUSamples
	}

	if nc.EvictionPolicy == "" {
		nc.EvictionPolicy = "NONE"
	}

	if nc.MaxInuse < 0 {
		nc.MaxInuse = 0
	}

	if nc.MaxKeys < 0 {
		nc.MaxKeys = 0
	}

	return nil
}

func (nc *NearCache) Validate() error { return nil }

var _ IConfig = (*NearCache)(nil)
