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
	"time"

	"github.com/buraksezer/olric/config/internal/loader"
	"github.com/pkg/errors"
)

// EvictionPolicy denotes eviction policy. Currently: LRU or NONE.
type EvictionPolicy string

// note on DMapCacheConfig and CacheConfig:
// golang doesn't provide the typical notion of inheritance.
// because of that I preferred to define the types explicitly.

// DMapCacheConfig denotes cache configuration for a particular dmap.
type DMapCacheConfig struct {
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the dmap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a dmap instance.
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
// DMapCacheConfig for a particular dmap. Don't set this if you use Olric as an ordinary key/value store.
type CacheConfig struct {
	// NumEvictionWorkers denotes the number of goroutines that's used to find keys for eviction.
	NumEvictionWorkers int64
	// MaxIdleDuration denotes maximum time for each entry to stay idle in the dmap.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period exceeds
	// this limit are expired and evicted automatically. An entry is idle if no Get,
	// Put, PutEx, Expire, PutIf, PutIfEx on it. Configuration of MaxIdleDuration
	// feature varies by preferred deployment method.
	MaxIdleDuration time.Duration

	// TTLDuration is useful to set a default TTL for every key/value pair a dmap instance.
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

	// DMapConfigs is useful to set custom cache config per dmap instance.
	DMapConfigs map[string]DMapCacheConfig
}

func processCacheConfig(c *loader.Loader) (*CacheConfig, error) {
	res := &CacheConfig{}
	if c.Cache.MaxIdleDuration != "" {
		maxIdleDuration, err := time.ParseDuration(c.Cache.MaxIdleDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse cache.MaxIdleDuration")
		}
		res.MaxIdleDuration = maxIdleDuration
	}
	if c.Cache.TTLDuration != "" {
		ttlDuration, err := time.ParseDuration(c.Cache.TTLDuration)
		if err != nil {
			return nil, errors.WithMessage(err, "failed to parse cache.TTLDuration")
		}
		res.TTLDuration = ttlDuration
	}
	res.NumEvictionWorkers = c.Cache.NumEvictionWorkers
	res.MaxKeys = c.Cache.MaxKeys
	res.MaxInuse = c.Cache.MaxInuse
	res.EvictionPolicy = EvictionPolicy(c.Cache.EvictionPolicy)
	res.LRUSamples = c.Cache.LRUSamples
	if c.DMaps != nil {
		res.DMapConfigs = make(map[string]DMapCacheConfig)
		for name, dc := range c.DMaps {
			cc := DMapCacheConfig{
				MaxInuse:       dc.MaxInuse,
				MaxKeys:        dc.MaxKeys,
				EvictionPolicy: EvictionPolicy(dc.EvictionPolicy),
				LRUSamples:     dc.LRUSamples,
			}
			if dc.MaxIdleDuration != "" {
				maxIdleDuration, err := time.ParseDuration(dc.MaxIdleDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse cache.%s.MaxIdleDuration", name)
				}
				cc.MaxIdleDuration = maxIdleDuration
			}
			if dc.TTLDuration != "" {
				ttlDuration, err := time.ParseDuration(dc.TTLDuration)
				if err != nil {
					return nil, errors.WithMessagef(err, "failed to parse cache.%s.TTLDuration", name)
				}
				cc.TTLDuration = ttlDuration
			}
			res.DMapConfigs[name] = cc
		}
	}
	return res, nil
}
