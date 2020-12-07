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
	"context"
	"testing"
	"time"

	"github.com/buraksezer/olric/config"
)

func TestSetCacheConfiguration(t *testing.T) {
	db, err := newDB(testSingleReplicaConfig())
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		err = db.Shutdown(context.Background())
		if err != nil {
			db.log.V(2).Printf("[ERROR] Failed to shutdown Olric: %v", err)
		}
	}()

	t.Run("Custom config config", func(t *testing.T) {
		// Config for DMap foobar
		db.config.DMaps = &config.DMaps{
			Custom: make(map[string]config.DMap),
		}
		cc := config.DMap{
			MaxIdleDuration: time.Second,
			TTLDuration:     time.Second,
			MaxKeys:         10,
			MaxInuse:        15,
			LRUSamples:      10,
			EvictionPolicy:  config.LRUEviction,
		}
		db.config.DMaps.Custom["foobar"] = cc
		hkey := db.getHKey("foobar", "barfoo")
		dm, err := db.getDMap("foobar", hkey)
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}
		err = db.setDMapConfiguration(dm, "foobar")
		if err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		if dm.config.maxIdleDuration != cc.MaxIdleDuration {
			t.Fatalf("Expected MaxIdleDuration: %v. Got: %v",
				cc.MaxIdleDuration, dm.config.maxIdleDuration)
		}

		if dm.config.ttlDuration != cc.TTLDuration {
			t.Fatalf("Expected TTLDuration: %v. Got: %v",
				cc.TTLDuration, dm.config.ttlDuration)
		}

		if dm.config.maxKeys != cc.MaxKeys {
			t.Fatalf("Expected MaxKeys: %v. Got: %v",
				cc.MaxKeys, dm.config.maxKeys)
		}

		if dm.config.maxInuse != cc.MaxInuse {
			t.Fatalf("Expected MaxInuse: %v. Got: %v",
				cc.MaxInuse, dm.config.maxInuse)
		}

		if dm.config.lruSamples != cc.LRUSamples {
			t.Fatalf("Expected LRUSamples: %v. Got: %v",
				cc.LRUSamples, dm.config.lruSamples)
		}

		if dm.config.evictionPolicy != cc.EvictionPolicy {
			t.Fatalf("Expected EvictionPolicy: %v. Got: %v",
				cc.EvictionPolicy, dm.config.evictionPolicy)
		}
	})
}
