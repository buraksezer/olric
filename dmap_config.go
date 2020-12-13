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

// TODO: accessLog should be moved to its own struct
// TODO: dmapConfig will be renamed dmap.Config after refactoring. See #70 for details.

// dmapConfig keeps DMap config control parameters and access-log for keys in a dmap.
type dmapConfig struct {
	sync.RWMutex // protects accessLog

	maxIdleDuration time.Duration
	ttlDuration     time.Duration
	maxKeys         int
	maxInuse        int
	accessLog       map[uint64]int64
	lruSamples      int
	evictionPolicy  config.EvictionPolicy
	storageEngine   string
}

func (db *Olric) setDMapConfiguration(dm *dmap, name string) error {
	// Try to set config configuration for this dmap.
	dm.config.maxIdleDuration = db.config.DMaps.MaxIdleDuration
	dm.config.ttlDuration = db.config.DMaps.TTLDuration
	dm.config.maxKeys = db.config.DMaps.MaxKeys
	dm.config.maxInuse = db.config.DMaps.MaxInuse
	dm.config.lruSamples = db.config.DMaps.LRUSamples
	dm.config.evictionPolicy = db.config.DMaps.EvictionPolicy
	if db.config.DMaps.StorageEngine != "" {
		dm.config.storageEngine = db.config.DMaps.StorageEngine
	}

	if db.config.DMaps.Custom != nil {
		// config.DMap struct can be used for fine-grained control.
		c, ok := db.config.DMaps.Custom[name]
		if ok {
			if dm.config.maxIdleDuration != c.MaxIdleDuration {
				dm.config.maxIdleDuration = c.MaxIdleDuration
			}
			if dm.config.ttlDuration != c.TTLDuration {
				dm.config.ttlDuration = c.TTLDuration
			}
			if dm.config.evictionPolicy != c.EvictionPolicy {
				dm.config.evictionPolicy = c.EvictionPolicy
			}
			if dm.config.maxKeys != c.MaxKeys {
				dm.config.maxKeys = c.MaxKeys
			}
			if dm.config.maxInuse != c.MaxInuse {
				dm.config.maxInuse = c.MaxInuse
			}
			if dm.config.lruSamples != c.LRUSamples {
				dm.config.lruSamples = c.LRUSamples
			}
			if dm.config.evictionPolicy != c.EvictionPolicy {
				dm.config.evictionPolicy = c.EvictionPolicy
			}
			if c.StorageEngine != "" && dm.config.storageEngine != c.StorageEngine {
				dm.config.storageEngine = c.StorageEngine
			}
		}
	}

	if dm.config.evictionPolicy == config.LRUEviction || dm.config.maxIdleDuration != 0 {
		dm.config.accessLog = make(map[uint64]int64)
	}

	// TODO: Create a new function to verify config config.
	if dm.config.evictionPolicy == config.LRUEviction {
		if dm.config.maxInuse <= 0 && dm.config.maxKeys <= 0 {
			return fmt.Errorf("maxInuse or maxKeys have to be greater than zero")
		}
		// set the default value.
		if dm.config.lruSamples == 0 {
			dm.config.lruSamples = config.DefaultLRUSamples
		}
	}
	return nil
}
