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

package server

import (
	"context"
	"fmt"
	"github.com/buraksezer/olric/config"
	"github.com/go-redis/redis/v8"
	"sync"
)

type Client struct {
	mu sync.RWMutex

	config  *config.Client
	clients map[string]*redis.Client
}

func NewClient(c *config.Client) *Client {
	if c == nil {
		c = config.NewClient()
		err := c.Sanitize()
		if err != nil {
			panic(fmt.Sprintf("failed to sanitize client config: %s", err))
		}
	}
	return &Client{
		config:  c,
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

	opt := c.config.RedisOptions()
	opt.Addr = addr
	rc = redis.NewClient(opt)
	c.clients[addr] = rc
	// TODO: Remove unhealthy redis client periodically.
	// TODO: Send a pig command after calling NewClient.
	return rc
}

func (c *Client) Pick() (*redis.Client, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// TODO: Implement round-robin scheduling algorithm.
	for _, rc := range c.clients {
		return rc, nil
	}
	return nil, fmt.Errorf("no available client found")
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

func (c *Client) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, rc := range c.clients {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := rc.Close(); err != nil {
			return err
		}
		delete(c.clients, addr)
	}

	return nil
}
