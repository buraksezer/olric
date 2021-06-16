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
	"io"
	"os"

	"github.com/buraksezer/connpool"
	"github.com/buraksezer/olric/internal/protocol"
)

func readFromStream(conn io.ReadWriteCloser, bufCh chan<- protocol.EncodeDecoder, errCh chan<- error) {
	f := func() error {
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)

		header, err := protocol.ReadMessage(conn, buf)
		if err != nil {
			return err
		}

		msg, err := prepareRequest(header, buf)
		if err != nil {
			return err
		}

		err = msg.Decode()
		if err != nil {
			return err
		}
		bufCh <- msg
		return nil
	}

	for {
		if err := f(); err != nil {
			errCh <- err
			break
		}
	}
}

// CreateStream creates a new Stream connection which provides a bidirectional communication channel between Olric nodes and clients.
func (c *Client) CreateStream(ctx context.Context, addr string, read chan<- protocol.EncodeDecoder, write <-chan protocol.EncodeDecoder) error {
	p, err := c.pool(addr)
	if err != nil {
		return err
	}

	conn, err := p.Get(ctx)
	if err != nil {
		return err
	}

	defer func() {
		// marks the connection not usable any more, to let the pool close it instead of returning it to pool.
		pc, _ := conn.(*connpool.PoolConn)
		pc.MarkUnusable()
		if err = pc.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "[ERROR] Failed to close connection: %v", err)
		}
	}()

	// Create a new byte stream
	b := bufferPool.Get()
	defer bufferPool.Put(b)

	req := protocol.NewStreamMessage(protocol.OpCreateStream)
	req.SetBuffer(b)
	err = req.Encode()
	if err != nil {
		return err
	}
	if _, err = req.Buffer().WriteTo(conn); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	bufCh := make(chan protocol.EncodeDecoder, 1)

	go readFromStream(conn, bufCh, errCh)
	for {
		select {
		case msg := <-write:
			b.Reset()
			msg.SetBuffer(b)
			if err = msg.Encode(); err != nil {
				return err
			}
			if _, err = msg.Buffer().WriteTo(conn); err != nil {
				return err
			}
		case buf := <-bufCh:
			read <- buf
		case err = <-errCh:
			return err
		case <-ctx.Done():
			return nil
		}
	}
}
