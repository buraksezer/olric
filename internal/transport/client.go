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

const (
	DefaultDialTimeout  = 5 * time.Second
	DefaultReadTimeout  = 3 * time.Second
	DefaultWriteTimeout = 3 * time.Second
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
	// List of host:port address.
	Addrs []string

	// Dial timeout for establishing new connections.
	// Default is DefaultDialTimeout
	DialTimeout time.Duration

	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is DefaultReadTimeout
	ReadTimeout time.Duration

	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is DefaultWriteTimeout
	WriteTimeout time.Duration

	// KeepAlive specifies the interval between keep-alive
	// probes for an active network connection.
	// If zero, keep-alive probes are sent with a default value
	// (currently 15 seconds), if supported by the protocol and operating
	// system. Network protocols or operating systems that do
	// not support keep-alives ignore this field.
	// If negative, keep-alive probes are disabled.
	KeepAlive time.Duration

	// Minimum TCP connection count in the pool for a host:port
	MinConn int

	// Maximum TCP connection count in the pool for a host:port
	MaxConn int
}

func (cc *ClientConfig) sanitize() {
	if cc.DialTimeout == 0 {
		cc.DialTimeout = DefaultDialTimeout
	}

	switch cc.ReadTimeout {
	case -1:
		cc.ReadTimeout = 0
	case 0:
		cc.ReadTimeout = DefaultReadTimeout
	}

	switch cc.WriteTimeout {
	case -1:
		cc.WriteTimeout = 0
	case 0:
		cc.WriteTimeout = DefaultWriteTimeout
	}

	if cc.MaxConn == 0 {
		cc.MaxConn = 1
	}
}

func (cc *ClientConfig) hasTimeout() bool {
	return cc.ReadTimeout > 0 || cc.WriteTimeout > 0
}

// NewClient returns a new Client.
func NewClient(cc *ClientConfig) *Client {
	if cc == nil {
		panic("ClientConfig cannot be nil")
	}
	cc.sanitize()

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
	// Reset pool
	c.pools = make(map[string]pool.Pool)
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
func (c *Client) pool(addr string) (pool.Pool, error) {
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

func (c *Client) conn(addr string) (net.Conn, error) {
	cpool, err := c.pool(addr)
	if err != nil {
		return nil, err
	}

	conn, err := cpool.Get()
	if err != nil {
		return nil, err
	}

	if c.config.hasTimeout() {
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
	if c.config.hasTimeout() {
		c.teardownConnWithTimeout(rawConn.(*ConnWithTimeout), dead)
		return
	}
	pc, _ := rawConn.(*pool.PoolConn)
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

// Request initiates a request-response cycle to randomly selected host.
func (c *Client) Request(req protocol.EncodeDecoder) (protocol.EncodeDecoder, error) {
	addr := c.roundrobin.Get()
	return c.RequestTo(addr, req)
}
