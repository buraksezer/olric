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

package transport

import (
	"net"
	"time"

	"github.com/buraksezer/connpool"
)

// ConnWithTimeout denotes a composite type which can used to implement i/o timeout feature for TCP sockets.
type ConnWithTimeout struct {
	net.Conn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewConnWithTimeout returns a new ConnWithTimeout instance.
func NewConnWithTimeout(conn net.Conn, readTimeout, writeTimeout time.Duration) *ConnWithTimeout {
	return &ConnWithTimeout{
		Conn:         conn,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

// Read reads data from the connection and calls net.SetReadDeadline before reading.
func (c *ConnWithTimeout) Read(b []byte) (int, error) {
	err := c.Conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	if err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

// Write writes data to the connection and calls net.SetWriteDeadline before writing.
func (c *ConnWithTimeout) Write(b []byte) (int, error) {
	err := c.Conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	if err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

// UnsetDeadline unsets Read/Write deadline directive.
func (c *ConnWithTimeout) UnsetDeadline() error {
	return c.Conn.SetDeadline(time.Time{})
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
// Wrapper around connpool.PoolConn.MarkUnusable
func (c *ConnWithTimeout) MarkUnusable() {
	if conn, ok := c.Conn.(*connpool.PoolConn); ok {
		conn.MarkUnusable()
	}
}

// Close closes the connection.
func (c *ConnWithTimeout) Close() error {
	conn, ok := c.Conn.(*connpool.PoolConn)
	if ok {
		return conn.Close()
	}
	return c.Conn.Close()
}
