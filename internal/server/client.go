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

package server

import (
	"sync"

	"github.com/go-redis/redis/v8"
)

type Client struct {
	mu sync.RWMutex

	options *redis.Options
	clients map[string]*redis.Client
}

func NewClient(opt *redis.Options) *Client {
	return &Client{
		options: opt,
		clients: make(map[string]*redis.Client),
	}
}

func (c *Client) Get(addr string) *redis.Client {
	c.mu.RLock()
	rc, ok := c.clients[addr]
	if ok {
		c.mu.RUnlock()
		return rc
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	opt := &redis.Options{
		Network:            "tcp",
		Addr:               addr,
		Dialer:             c.options.Dialer,
		OnConnect:          c.options.OnConnect,
		MaxRetries:         c.options.MaxRetries,
		MinRetryBackoff:    c.options.MinRetryBackoff,
		MaxRetryBackoff:    c.options.MaxRetryBackoff,
		DialTimeout:        c.options.DialTimeout,
		ReadTimeout:        c.options.ReadTimeout,
		WriteTimeout:       c.options.WriteTimeout,
		PoolFIFO:           c.options.PoolFIFO,
		PoolSize:           c.options.PoolSize,
		MinIdleConns:       c.options.MinIdleConns,
		MaxConnAge:         c.options.MaxConnAge,
		PoolTimeout:        c.options.PoolTimeout,
		IdleTimeout:        c.options.IdleTimeout,
		IdleCheckFrequency: c.options.IdleCheckFrequency,
		TLSConfig:          c.options.TLSConfig,
		Limiter:            c.options.Limiter,
	}

	rc = redis.NewClient(opt)
	c.clients[addr] = rc
	rc.PoolStats()
	return rc
}

func (c *Client) Close(addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	rc, ok := c.clients[addr]
	if ok {
		err := rc.Close()
		if err != nil {
			return err
		}
		delete(c.clients, addr)
	}

	return nil
}
