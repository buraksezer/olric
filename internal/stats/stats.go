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

package stats

import "sync/atomic"

type Counter interface {
	Tag() string
	Increase()
	Read() interface{}
}

type Gauge interface {
	Tag() string
	Increase()
	Decrease()
	Read() interface{}
}

type Int64Counter struct {
	tag     string
	counter int64
}

func NewInt64Counter(tag string) *Int64Counter {
	return &Int64Counter{
		tag: tag,
	}
}

func (c *Int64Counter) Tag() string {
	return c.tag
}

func (c *Int64Counter) Increase() {
	atomic.AddInt64(&c.counter, 1)
}

func (c *Int64Counter) Read() interface{} {
	return atomic.LoadInt64(&c.counter)
}

type Int64Gauge struct {
	tag     string
	counter int64
}

func NewInt64Gauge(tag string) *Int64Gauge {
	return &Int64Gauge{
		tag: tag,
	}
}

func (c *Int64Gauge) Tag() string {
	return c.tag
}

func (c *Int64Gauge) Increase() {
	atomic.AddInt64(&c.counter, 1)
}

func (c *Int64Gauge) Decrease() {
	atomic.AddInt64(&c.counter, -1)
}

func (c *Int64Gauge) Read() interface{} {
	return atomic.LoadInt64(&c.counter)
}
