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

package client

import (
	"bytes"
	"io"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/hashicorp/go-multierror"
)

// Pipeline implements pipelining feature for Olric Binary Protocol.
// It enables to send multiple commands to the server without
// waiting for the replies at all, and finally read the replies
// in a single step. All methods are thread-safe. So you can call them in
// different goroutines safely.
type Pipeline struct {
	c   *Client
	m   sync.Mutex
	buf *bytes.Buffer
}

// NewPipeline returns a new Pipeline.
func (c *Client) NewPipeline() *Pipeline {
	return &Pipeline{
		c:   c,
		buf: bytes.NewBuffer(nil),
	}
}

// Put appends a Put command to the underlying buffer with the given parameters.
func (p *Pipeline) Put(dmap, key string, value interface{}) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}

	req := protocol.NewDMapMessage(protocol.OpPut)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutExtra{Timestamp: time.Now().UnixNano()})
	return req.Encode()
}

// PutEx appends a PutEx command to the underlying buffer with the given parameters.
func (p *Pipeline) PutEx(dmap, key string, value interface{}, timeout time.Duration) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}

	req := protocol.NewDMapMessage(protocol.OpPutEx)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutExExtra{
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	return req.Encode()
}

// Get appends a Get command to the underlying buffer with the given parameters.
func (p *Pipeline) Get(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	req := protocol.NewDMapMessage(protocol.OpGet)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	return req.Encode()
}

// Delete appends a Delete command to the underlying buffer with the given parameters.
func (p *Pipeline) Delete(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	req := protocol.NewDMapMessage(protocol.OpDelete)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	return req.Encode()
}

func (p *Pipeline) incrOrDecr(opcode protocol.OpCode, dmap, key string, delta int) error {
	p.m.Lock()
	defer p.m.Unlock()

	value, err := p.c.serializer.Marshal(delta)
	if err != nil {
		return err
	}
	req := protocol.NewDMapMessage(opcode)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(value)
	req.SetExtra(protocol.AtomicExtra{Timestamp: time.Now().UnixNano()})
	return req.Encode()
}

// Incr appends an Incr command to the underlying buffer with the given parameters.
func (p *Pipeline) Incr(dmap, key string, delta int) error {
	return p.incrOrDecr(protocol.OpIncr, dmap, key, delta)
}

// Decr appends a Decr command to the underlying buffer with the given parameters.
func (p *Pipeline) Decr(dmap, key string, delta int) error {
	return p.incrOrDecr(protocol.OpDecr, dmap, key, delta)
}

// GetPut appends a GetPut command to the underlying buffer with the given parameters.
func (p *Pipeline) GetPut(dmap, key string, value interface{}) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}

	req := protocol.NewDMapMessage(protocol.OpGetPut)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.AtomicExtra{Timestamp: time.Now().UnixNano()})
	return req.Encode()
}

// Destroy appends a Destroy command to the underlying buffer with the given parameters.
func (p *Pipeline) Destroy(dmap string) error {
	p.m.Lock()
	defer p.m.Unlock()

	req := protocol.NewDMapMessage(protocol.OpDestroy)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	return req.Encode()
}

// PutIf appends a PutIf command to the underlying buffer.
//
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (p *Pipeline) PutIf(dmap, key string, value interface{}, flags int16) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}

	req := protocol.NewDMapMessage(protocol.OpPutIf)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutIfExtra{
		Flags:     flags,
		Timestamp: time.Now().UnixNano(),
	})
	return req.Encode()
}

// PutIfEx appends a PutIfEx command to the underlying buffer.
//
// Flag argument currently has two different options:
//
// olric.IfNotFound: Only set the key if it does not already exist.
// It returns olric.ErrFound if the key already exist.
//
// olric.IfFound: Only set the key if it already exist.
// It returns olric.ErrKeyNotFound if the key does not exist.
func (p *Pipeline) PutIfEx(dmap, key string, value interface{}, timeout time.Duration, flags int16) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}

	req := protocol.NewDMapMessage(protocol.OpPutIfEx)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetValue(data)
	req.SetExtra(protocol.PutIfExExtra{
		Flags:     flags,
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	return req.Encode()
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (p *Pipeline) Expire(dmap, key string, timeout time.Duration) error {
	p.m.Lock()
	defer p.m.Unlock()

	req := protocol.NewDMapMessage(protocol.OpExpire)
	req.SetBuffer(p.buf)
	req.SetDMap(dmap)
	req.SetKey(key)
	req.SetExtra(protocol.ExpireExtra{
		TTL:       timeout.Nanoseconds(),
		Timestamp: time.Now().UnixNano(),
	})
	return req.Encode()
}

// Flush flushes all the commands to the server using a single write call.
func (p *Pipeline) Flush() ([]PipelineResponse, error) {
	p.m.Lock()
	defer p.m.Unlock()
	defer p.buf.Reset()

	req := protocol.NewPipelineMessage(protocol.OpPipeline)
	req.SetValue(p.buf.Bytes())
	resp, err := p.c.request(req)
	if err != nil {
		return nil, err
	}

	// Decode the pipelined messages from pipeline response.
	conn := protocol.NewBytesToConn(resp.Value())
	var responses []PipelineResponse
	var resErr error

	flushMessage := func() error {
		buf := bufferPool.Get()
		defer bufferPool.Put(buf)

		_, err = protocol.ReadMessage(conn, buf)
		pres := protocol.NewDMapMessageFromRequest(buf)
		err = pres.Decode()
		if err != nil {
			return err
		}
		pr := PipelineResponse{
			Client:   p.c,
			response: pres,
		}
		responses = append(responses, pr)
		return nil
	}
	for {
		err := flushMessage()
		if err == io.EOF {
			break
		}
		if err != nil {
			resErr = multierror.Append(resErr, err)
			continue
		}
	}
	return responses, resErr
}
