// Copyright 2018-2021 Burak Sezer
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

package dmap

import (
	"github.com/buraksezer/olric/config"
	"testing"
	"time"
)

func TestDMap_Config(t *testing.T) {
	c := config.New("local")
	// Config for all new DMaps
	c.DMaps.NumEvictionWorkers = 1
	c.DMaps.TTLDuration = 100 * time.Second
	c.DMaps.MaxKeys = 100000
	c.DMaps.MaxInuse = 1000000
	c.DMaps.LRUSamples = 10
	c.DMaps.EvictionPolicy = config.LRUEviction
	c.DMaps.StorageEngine = config.DefaultStorageEngine

	// Config for specified DMaps
	c.DMaps.Custom = map[string]config.DMap{"foobar": {
		MaxIdleDuration: 60 * time.Second,
		TTLDuration:     300 * time.Second,
		MaxKeys:         500000,
		LRUSamples:      20,
		EvictionPolicy:  "NONE",
		StorageEngine:   "olric.document-store",
	}}

	dc := dmapConfig{}
	err := dc.load(c.DMaps, "mydmap")
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	if dc.ttlDuration != c.DMaps.TTLDuration {
		t.Fatalf("Expected TTLDuration: %v. Got: %v", c.DMaps.TTLDuration, dc.ttlDuration)
	}

	if dc.maxKeys != c.DMaps.MaxKeys {
		t.Fatalf("Expected MaxKeys: %v. Got: %v", c.DMaps.MaxKeys, dc.maxKeys)
	}
	if dc.maxInuse != c.DMaps.MaxInuse {
		t.Fatalf("Expected MaxInuse: %v. Got: %v", c.DMaps.MaxInuse, dc.maxInuse)
	}
	if dc.lruSamples != c.DMaps.LRUSamples {
		t.Fatalf("Expected LRUSamples: %v. Got: %v", c.DMaps.LRUSamples, dc.lruSamples)
	}
	if dc.evictionPolicy != c.DMaps.EvictionPolicy {
		t.Fatalf("Expected EvictionPolicy: %v. Got: %v", c.DMaps.EvictionPolicy, dc.evictionPolicy)
	}
	if dc.storageEngine != c.DMaps.StorageEngine {
		t.Fatalf("Expected StorageEngine: %v. Got: %v", c.DMaps.StorageEngine, dc.storageEngine)
	}

	t.Run("Custom config", func(t *testing.T) {
		dcc := dmapConfig{}
		err := dcc.load(c.DMaps, "foobar")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		if dcc.ttlDuration != c.DMaps.Custom["foobar"].TTLDuration {
			t.Fatalf("Expected TTLDuration: %v. Got: %v", c.DMaps.Custom["foobar"].TTLDuration, dcc.ttlDuration)
		}

		if dcc.maxKeys != c.DMaps.Custom["foobar"].MaxKeys {
			t.Fatalf("Expected MaxKeys: %v. Got: %v", c.DMaps.Custom["foobar"].MaxKeys, dcc.maxKeys)
		}
		if dcc.maxInuse != c.DMaps.Custom["foobar"].MaxInuse {
			t.Fatalf("Expected MaxInuse: %v. Got: %v", c.DMaps.Custom["foobar"].MaxInuse, dcc.maxInuse)
		}
		if dcc.lruSamples != c.DMaps.Custom["foobar"].LRUSamples {
			t.Fatalf("Expected LRUSamples: %v. Got: %v", c.DMaps.Custom["foobar"].LRUSamples, dcc.lruSamples)
		}
		if dcc.evictionPolicy != c.DMaps.Custom["foobar"].EvictionPolicy {
			t.Fatalf("Expected EvictionPolicy: %v. Got: %v", c.DMaps.Custom["foobar"].EvictionPolicy, dcc.evictionPolicy)
		}
		if dcc.storageEngine != c.DMaps.Custom["foobar"].StorageEngine {
			t.Fatalf("Expected StorageEngine: %v. Got: %v", c.DMaps.Custom["foobar"].StorageEngine, dcc.storageEngine)
		}

	})
}
