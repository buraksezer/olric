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

package client

import (
	"errors"
	"sync"
)

// roundRobin implements quite simple round-robin scheduling algorithm to distribute load fairly between servers.
type roundRobin struct {
	sync.Mutex

	current int
	addrs   []string
}

// newRoundRobin returns a new roundRobin.
func newRoundRobin(addrs []string) *roundRobin {
	return &roundRobin{
		current: 0,
		addrs:   addrs,
	}
}

// get returns a host address.
func (r *roundRobin) get() string {
	r.Lock()
	defer r.Unlock()

	if r.current >= len(r.addrs) {
		r.current %= len(r.addrs)
	}

	addr := r.addrs[r.current]
	r.current++
	return addr
}

// add adds a new address to the Round-Robin scheduler.
func (r *roundRobin) add(addr string) {
	r.Lock()
	defer r.Unlock()

	r.addrs = append(r.addrs, addr)
}

// delete deletes an address from the Round-Robin scheduler.
func (r *roundRobin) delete(addr string) error {
	r.Lock()
	defer r.Unlock()

	for i, item := range r.addrs {
		if item == addr {
			if len(r.addrs) == 1 {
				return errors.New("address cannot be removed")
			}
			r.addrs = append(r.addrs[:i], r.addrs[i+1:]...)
			return nil
		}
	}
	// not found
	return nil
}

// length returns the count of addresses
func (r *roundRobin) length() int {
	r.Lock()
	defer r.Unlock()

	return len(r.addrs)
}
