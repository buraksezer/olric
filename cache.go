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

package olric

import (
	"fmt"
	"sync"
	"time"

	"github.com/buraksezer/olric/config"
)

// cache keeps cache control parameters and access-log for keys in a dmap.
type cache struct {
	sync.RWMutex // protects accessLog

	maxIdleDuration time.Duration
	ttlDuration     time.Duration
	maxKeys         int
	maxInuse        int
	accessLog       map[uint64]int64
	lruSamples      int
	evictionPolicy  config.EvictionPolicy
}

func (db *Olric) setCacheConfiguration(dm *dmap, name string) error {
	// Try to set cache configuration for this dmap.
	dm.cache = &cache{}
	dm.cache.maxIdleDuration = db.config.Cache.MaxIdleDuration
	dm.cache.ttlDuration = db.config.Cache.TTLDuration
	dm.cache.maxKeys = db.config.Cache.MaxKeys
	dm.cache.maxInuse = db.config.Cache.MaxInuse
	dm.cache.lruSamples = db.config.Cache.LRUSamples
	dm.cache.evictionPolicy = db.config.Cache.EvictionPolicy

	if db.config.Cache.DMapConfigs != nil {
		// config.DMapCacheConfig struct can be used for fine-grained control.
		c, ok := db.config.Cache.DMapConfigs[name]
		if ok {
			if dm.cache.maxIdleDuration != c.MaxIdleDuration {
				dm.cache.maxIdleDuration = c.MaxIdleDuration
			}
			if dm.cache.ttlDuration != c.TTLDuration {
				dm.cache.ttlDuration = c.TTLDuration
			}
			if dm.cache.evictionPolicy != c.EvictionPolicy {
				dm.cache.evictionPolicy = c.EvictionPolicy
			}
			if dm.cache.maxKeys != c.MaxKeys {
				dm.cache.maxKeys = c.MaxKeys
			}
			if dm.cache.maxInuse != c.MaxInuse {
				dm.cache.maxInuse = c.MaxInuse
			}
			if dm.cache.lruSamples != c.LRUSamples {
				dm.cache.lruSamples = c.LRUSamples
			}
			if dm.cache.evictionPolicy != c.EvictionPolicy {
				dm.cache.evictionPolicy = c.EvictionPolicy
			}
		}
	}

	if dm.cache.evictionPolicy == config.LRUEviction || dm.cache.maxIdleDuration != 0 {
		dm.cache.accessLog = make(map[uint64]int64)
	}

	// TODO: Create a new function to verify cache config.
	if dm.cache.evictionPolicy == config.LRUEviction {
		if dm.cache.maxInuse <= 0 && dm.cache.maxKeys <= 0 {
			return fmt.Errorf("maxInuse or maxKeys have to be greater than zero")
		}
		// set the default value.
		if dm.cache.lruSamples == 0 {
			dm.cache.lruSamples = config.DefaultLRUSamples
		}
	}
	return nil
}
