// Copyright 2019 Burak Sezer
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

package transport

import "sync"

// RoundRobin implements quite simple round-robin algorithm to distribute load fairly between servers.
type RoundRobin struct {
	sync.Mutex

	current int
	addrs   []string
}

// NewRoundRobin returns a new RoundRobin.
func NewRoundRobin(addrs []string) *RoundRobin {
	return &RoundRobin{
		current: 0,
		addrs:   addrs,
	}
}

// Get returns a host address.
func (r *RoundRobin) Get() string {
	r.Lock()
	defer r.Unlock()

	if r.current >= len(r.addrs) {
		r.current %= len(r.addrs)
	}

	addr := r.addrs[r.current]
	r.current++
	return addr
}
