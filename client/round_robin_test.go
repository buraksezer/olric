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

package client

import (
	"testing"
)

func TestRoundRobin(t *testing.T) {
	addrs := []string{"127.0.0.1:2323", "127.0.0.1:4556", "127.0.0.1:7889"}
	r := newRoundRobin(addrs)

	t.Run("get", func(t *testing.T) {
		items := make(map[string]int)
		for i := 0; i < r.length(); i++ {
			item := r.get()
			items[item]++
		}
		if len(items) != r.length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.length(), len(items))
		}
	})

	t.Run("add", func(t *testing.T) {
		addr := "127.0.0.1:3320"
		r.add(addr)
		items := make(map[string]int)
		for i := 0; i < r.length(); i++ {
			item := r.get()
			items[item]++
		}
		if _, ok := items[addr]; !ok {
			t.Fatalf("Addr not processed: %s", addr)
		}
		if len(items) != r.length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.length(), len(items))
		}
	})

	t.Run("delete", func(t *testing.T) {
		addr := "127.0.0.1:7889"
		if err := r.delete(addr); err != nil {
			t.Fatalf("Expected nil. Got: %v", err)
		}

		items := make(map[string]int)
		for i := 0; i < r.length(); i++ {
			item := r.get()
			items[item]++
		}
		if _, ok := items[addr]; ok {
			t.Fatalf("Address stil exists: %s", addr)
		}
		if len(items) != r.length() {
			t.Fatalf("Expected item count: %d. Got: %d", r.length(), len(items))
		}
	})
}

func TestRoundRobin_Delete_NonExistent(t *testing.T) {
	addrs := []string{"127.0.0.1:2323", "127.0.0.1:4556", "127.0.0.1:7889"}
	r := newRoundRobin(addrs)

	var fresh []string
	fresh = append(fresh, addrs...)
	for i, addr := range fresh {
		if i+1 == len(addrs) {
			if err := r.delete(addr); err == nil {
				t.Fatal("Expected an error. Got: nil")
			}
		} else {
			if err := r.delete(addr); err != nil {
				t.Fatalf("Expected nil. Got: %v", err)
			}
		}
	}
}
