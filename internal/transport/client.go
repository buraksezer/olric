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
	"context"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/buraksezer/connpool"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/internal/protocol"
)

// Client is the client implementation for the internal TCP server.
// It maintains a connection pool and manages request-response cycle.
type Client struct {
	mu sync.RWMutex

	dialer *net.Dialer
	config *config.Client
	pools  map[string]connpool.Pool
}

// NewClient returns a new Client.
func NewClient(cc *config.Client) *Client {
	if cc == nil {
		panic("Client cannot be nil")
	}

	dialer := &net.Dialer{
		Timeout:   cc.DialTimeout,
		KeepAlive: cc.KeepAlive,
	}

	c := &Client{
		dialer: dialer,
		config: cc,
		pools:  make(map[string]connpool.Pool),
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
	// Reset pool
	c.pools = make(map[string]connpool.Pool)
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

// pool creates a new pool for a given addr or returns an exiting one.
func (c *Client) pool(addr string) (connpool.Pool, error) {
	factory := func() (net.Conn, error) {
		return c.dialer.Dial("tcp", addr)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	p, ok := c.pools[addr]
	if ok {
		return p, nil
	}

	p, err := connpool.NewChannelPool(c.config.MinConn, c.config.MaxConn, factory)
	if err != nil {
		return nil, err
	}
	c.pools[addr] = p
	return p, nil
}

func (c *Client) conn(addr string) (net.Conn, error) {
	p, err := c.pool(addr)
	if err != nil {
		return nil, err
	}

	// Use context.Background here because we dont want to change the default
	// behaviour.
	conn, err := p.Get(context.Background())
	if err != nil {
		return nil, err
	}

	if c.config.HasTimeout() {
		// Wrap the net.Conn to implement timeout logic
		conn = NewConnWithTimeout(conn, c.config.ReadTimeout, c.config.WriteTimeout)
	}
	return conn, err
}

func (c *Client) teardownConnWithTimeout(conn *ConnWithTimeout, dead bool) {
	if dead {
		conn.MarkUnusable()
	} else {
		if err := conn.UnsetDeadline(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to unset timeouts on TCP connection: %v", err)
		}
	}
	if err := conn.Close(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to close connection: %v", err)
	}
}

func (c *Client) teardownConn(rawConn net.Conn, dead bool) {
	if c.config.HasTimeout() {
		c.teardownConnWithTimeout(rawConn.(*ConnWithTimeout), dead)
		return
	}
	pc, _ := rawConn.(*connpool.PoolConn)
	pc.MarkUnusable()
	err := pc.Close()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to close connection: %v", err)
	}
}

// RequestTo initiates a request-response cycle to given host.
func (c *Client) RequestTo(addr string, req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	var dead bool
	defer func() {
		// We need the latest value of variable dead.
		c.teardownConn(conn, dead)
	}()

	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	req.SetBuffer(buf)
	err = req.Encode()
	if err != nil {
		return nil, err
	}
	_, err = req.Buffer().WriteTo(conn)
	if err != nil {
		dead = true
		return nil, err
	}

	// Await for the response
	buf.Reset()
	_, err = protocol.ReadMessage(conn, buf)
	if err != nil {
		// Failed to read message from the TCP socket. Close it.
		dead = true
		return nil, err
	}

	// Response is a shortcut to create a response message for the request.
	resp := req.Response(buf)
	err = resp.Decode()
	if err != nil {
		return nil, err
	}
	return resp, nil
}
