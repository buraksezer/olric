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

package roundrobin

import (
	"errors"
	"fmt"
)

var ErrEmptyInstance = errors.New("empty round-robin instance")

// RoundRobin implements quite simple round-robin scheduling algorithm to distribute load fairly between servers.
type RoundRobin struct {
	current int
	items   []string
}

// New returns a new RoundRobin instance.
func New(items []string) *RoundRobin {
	return &RoundRobin{
		current: 0,
		items:   items,
	}
}

// Get returns an item.
func (r *RoundRobin) Get() (string, error) {
	if r.current >= len(r.items) {
		r.current %= len(r.items)
	}

	if len(r.items) == 0 {
		return "", ErrEmptyInstance
	}

	if r.current >= len(r.items) {
		return "", fmt.Errorf("round-robin: corrupted internal state")
	}

	item := r.items[r.current]
	r.current++
	return item, nil
}

// Add adds a new item to the Round-Robin scheduler.
func (r *RoundRobin) Add(item string) {
	r.items = append(r.items, item)
}

// Delete deletes an item from the Round-Robin scheduler.
func (r *RoundRobin) Delete(i string) {
	for idx, item := range r.items {
		if item == i {
			r.items = append(r.items[:idx], r.items[idx+1:]...)
		}
	}
}

// Length returns the count of items
func (r *RoundRobin) Length() int {
	return len(r.items)
}
