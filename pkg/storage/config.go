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

type Config struct {
	m map[string]interface{}
	sync.RWMutex
}

func NewConfig(cfg map[string]interface{}) *Config {
	if cfg == nil {
		cfg = make(map[string]interface{})
	}
	return &Config{
		m: cfg,
	}
}

func (c *Config) Add(key string, value interface{}) {
	c.Lock()
	defer c.Unlock()
	c.m[key] = value
}

func (c *Config) Get(key string) (interface{}, error) {
	c.Lock()
	defer c.Unlock()
	value, ok := c.m[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return value, nil
}

func (c *Config) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.m, key)
}

func (c *Config) Copy() *Config {
	c.Lock()
	defer c.Unlock()
	n := &Config{
		m: make(map[string]interface{}),
	}
	for key, value := range c.m {
		c.m[key] = value
	}
	return n
}
