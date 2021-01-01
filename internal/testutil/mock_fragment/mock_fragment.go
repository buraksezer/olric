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

package mock_fragment

import (
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"sync"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/discovery"
)

type MockFragment struct {
	sync.RWMutex
	m map[string]interface{}
}

func New() *MockFragment {
	return &MockFragment{
		m: make(map[string]interface{}),
	}
}

func (f *MockFragment) Name() string {
	return "Mock-DMap"
}

func (f *MockFragment) Length() int {
	f.RLock()
	defer f.RUnlock()
	return len(f.m)
}

func (f *MockFragment) Put(key string, value interface{}) {
	f.Lock()
	defer f.Unlock()
	f.m[key] = value
}

func (f *MockFragment) Get(key string) interface{} {
	f.Lock()
	defer f.Unlock()
	return f.m[key]
}

func (f *MockFragment) Delete(key string) {
	f.Lock()
	defer f.Unlock()
	delete(f.m, key)
}

func (f *MockFragment) Fill() {
	n := 5
	b := make([]byte, n)
	randKey := func() string {
		if _, err := rand.Read(b); err != nil {
			panic(err)
		}
		return fmt.Sprintf("%X", b)
	}
	num := mrand.Intn(100)
	for i := 0; i < num; i++ {
		f.Put(randKey(), i)
	}
}

func (f *MockFragment) Move(_ uint64, _ partitions.Kind, _ string, _ discovery.Member) error {
	return nil
}

var _ partitions.Fragment = (*MockFragment)(nil)
