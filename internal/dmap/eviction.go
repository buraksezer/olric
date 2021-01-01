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

package dmap

func (dm *DMap) isKeyIdle(hkey uint64) bool {
	if dm.config == nil {
		return false
	}
	if dm.config.accessLog == nil || dm.config.maxIdleDuration.Nanoseconds() == 0 {
		return false
	}
	// Maximum time in seconds for each entry to stay idle in the map.
	// It limits the lifetime of the entries relative to the time of the last
	// read or write access performed on them. The entries whose idle period
	// exceeds this limit are expired and evicted automatically.
	dm.config.RLock()
	defer dm.config.RUnlock()
	t, ok := dm.config.accessLog[hkey]
	if !ok {
		return false
	}
	ttl := (dm.config.maxIdleDuration.Nanoseconds() + t) / 1000000
	return isKeyExpired(ttl)
}
