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

package transport

import (
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/pool"
)

// Client is the client implementation for the internal TCP server.
// It maintains a connection pool and manages request-response cycle.
type Client struct {
	mu sync.RWMutex

	dialer     *net.Dialer
	config     *ClientConfig
	roundrobin *RoundRobin
	pools      map[string]pool.Pool
}

// ClientConfig configuration parameters of the client.
type ClientConfig struct {
	Addrs       []string
	DialTimeout time.Duration
	KeepAlive   time.Duration
	MinConn     int
	MaxConn     int
}

// NewClient returns a new Client.
func NewClient(cc *ClientConfig) *Client {
	if cc == nil {
		panic("ClientConfig cannot be nil")
	}

	if cc.MaxConn == 0 {
		cc.MaxConn = 1
	}

	dialer := &net.Dialer{
		Timeout:   cc.DialTimeout,
		KeepAlive: cc.KeepAlive,
	}

	c := &Client{
		roundrobin: NewRoundRobin(cc.Addrs),
		dialer:     dialer,
		config:     cc,
		pools:      make(map[string]pool.Pool),
	}
	return c
}

// Close all the connections in the connection pool.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.pools {
		p.Close()
	}
}

// CloseWithAddr closes the connection for given addr, if any exists.
func (c *Client) CloseWithAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pools[addr]
	if !ok {
		p.Close()
		delete(c.pools, addr)
	}
}

// getPool creates a new pool for a given addr or returns an exiting one.
func (c *Client) getPool(addr string) (pool.Pool, error) {
	factory := func() (net.Conn, error) {
		return c.dialer.Dial("tcp", addr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cpool, ok := c.pools[addr]
	if ok {
		return cpool, nil
	}

	cpool, err := pool.NewChannelPool(c.config.MinConn, c.config.MaxConn, factory)
	if err != nil {
		return nil, err
	}
	c.pools[addr] = cpool
	return cpool, nil
}

// ClosePool closes the underlying connections in a pool,
// deletes from Olric's pools map and frees resources.
func (c *Client) ClosePool(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.pools[addr]
	if ok {
		// Close the pool. This closes the underlying connections.
		p.Close()
		// Delete from Olric.
		delete(c.pools, addr)
	}
}

// RequestTo initiates a request-response cycle to given host.
func (c *Client) RequestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	cpool, err := c.getPool(addr)
	if err != nil {
		return nil, err
	}

	conn, err := cpool.Get()
	if err != nil {
		return nil, err
	}

	var deadConn bool
	defer func() {
		var connErr error
		if !(deadConn) {
			// The conn returns to the pool
			connErr = conn.Close()
		} else {
			// marks the connection not usable any more, to let the pool close it instead of returning it to pool.
			pc, _ := conn.(*pool.PoolConn)
			pc.MarkUnusable()
			connErr = pc.Close()
		}
		if connErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to close connection: %v", connErr)
		}
	}()

	buf := bufferPool.Get()
	defer bufferPool.Put(buf)
	req.SetBuffer(buf)

	err = req.Encode()
	if err != nil {
		deadConn = true
		return nil, err
	}

	_, err = req.Buffer().WriteTo(conn)
	if err != nil {
		return nil, err
	}

	// Await for the response
	buf.Reset()
	_, err = protocol.ReadMessage(conn, buf)
	// Response is a shortcut to create a response message for the request.
	resp := req.Response(buf)
	err = resp.Decode()
	if err != nil {
		deadConn = true
		return nil, err
	}
	return resp, err
}

// Request initiates a request-response cycle to randomly selected host.
func (c *Client) Request(req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	addr := c.roundrobin.Get()
	return c.RequestTo(addr, req)
}
