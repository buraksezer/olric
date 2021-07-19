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

package config

import "time"

const (
	DefaultDialTimeout  = 5 * time.Second
	DefaultReadTimeout  = 3 * time.Second
	DefaultWriteTimeout = 3 * time.Second
)

// Client is configuration for TCP clients in Olric and the official Golang client.
type Client struct {
	// Timeout for TCP dial.
	//
	// The timeout includes name resolution, if required. When using TCP, and the host in the address parameter
	// resolves to multiple IP addresses, the timeout is spread over each consecutive dial, such that each is
	// given an appropriate fraction of the time to connect.
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

// NewClient returns a new configuration object for clients.
func NewClient() *Client {
	c := &Client{}
	c.Sanitize()
	return c
}

// Sanitize sanitizes the given configuration.
func (c *Client) Sanitize() {
	if c.DialTimeout <= 0 {
		c.DialTimeout = DefaultDialTimeout
	}

	switch c.ReadTimeout {
	case -1:
		c.ReadTimeout = 0
	case 0:
		c.ReadTimeout = DefaultReadTimeout
	}

	switch c.WriteTimeout {
	case -1:
		c.WriteTimeout = 0
	case 0:
		c.WriteTimeout = DefaultWriteTimeout
	}

	if c.MaxConn == 0 {
		c.MaxConn = 100
	}
}

func (c *Client) HasTimeout() bool {
	return c.ReadTimeout > 0 || c.WriteTimeout > 0
}
