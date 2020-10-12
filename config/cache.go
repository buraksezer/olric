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
