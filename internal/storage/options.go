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

package storage

import (
	"fmt"
	"sync"
)

type Options struct {
	m map[string]interface{}
	sync.RWMutex
}

func NewOptions() *Options {
	return &Options{
		m: make(map[string]interface{}),
	}
}

func (o *Options) Add(key string, value interface{}) {
	o.Lock()
	defer o.Unlock()
	o.m[key] = value
}

func (o *Options) Get(key string) (interface{}, error) {
	o.Lock()
	defer o.Unlock()
	value, ok := o.m[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return value, nil
}

func (o *Options) Delete(key string) {
	o.Lock()
	defer o.Unlock()
	delete(o.m, key)
}

func (o *Options) Copy() *Options {
	o.Lock()
	defer o.Unlock()
	n := &Options{
		m: make(map[string]interface{}),
	}
	for key, value := range o.m{
		o.m[key] = value
	}
	return n
}
