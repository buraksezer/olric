// Copyright 2018-2019 Burak Sezer
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

	"github.com/hashicorp/go-multierror"

	"github.com/buraksezer/olric/internal/protocol"
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
		buf: new(bytes.Buffer),
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
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpPut,
		},
		DMap:  dmap,
		Key:   key,
		Value: data,
	}
	return m.Write(p.buf)
}

// PutEx appends a PutEx command to the underlying buffer with the given parameters.
func (p *Pipeline) PutEx(dmap, key string, value interface{}, timeout time.Duration) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.c.serializer.Marshal(value)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpPutEx,
		},
		DMap:  dmap,
		Key:   key,
		Extra: protocol.PutExExtra{TTL: timeout.Nanoseconds()},
		Value: data,
	}
	return m.Write(p.buf)
}

// Get appends a Get command to the underlying buffer with the given parameters.
func (p *Pipeline) Get(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpGet,
		},
		DMap: dmap,
		Key:  key,
	}
	return m.Write(p.buf)
}

// Delete appends a Delete command to the underlying buffer with the given parameters.
func (p *Pipeline) Delete(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpDelete,
		},
		DMap: dmap,
		Key:  key,
	}
	return m.Write(p.buf)
}

func (p *Pipeline) incrOrDecr(opcode protocol.OpCode, dmap, key string, delta int) error {
	p.m.Lock()
	defer p.m.Unlock()

	value, err := p.c.serializer.Marshal(delta)
	if err != nil {
		return err
	}
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    opcode,
		},
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
	return m.Write(p.buf)
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
	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpGetPut,
		},
		DMap:  dmap,
		Key:   key,
		Value: data,
	}
	return m.Write(p.buf)
}

// Destroy appends a Destroy command to the underlying buffer with the given parameters.
func (p *Pipeline) Destroy(dmap string) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpDestroy,
		},
		DMap: dmap,
	}
	return m.Write(p.buf)
}

// LockWithTimeout appends a LockWithTimeout command to the underlying buffer with the given parameters.
func (p *Pipeline) LockWithTimeout(dmap, key string, timeout time.Duration) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpLockWithTimeout,
		},
		DMap:  dmap,
		Key:   key,
		Extra: protocol.LockWithTimeoutExtra{TTL: timeout.Nanoseconds()},
	}
	return m.Write(p.buf)
}

// Unlock appends an Unlock command to the underlying buffer with the given parameters.
func (p *Pipeline) Unlock(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpUnlock,
		},
		DMap: dmap,
		Key:  key,
	}
	return m.Write(p.buf)
}

// Flush flushes all the commands to the server using a single write call.
func (p *Pipeline) Flush() ([]PipelineResponse, error) {
	p.m.Lock()
	defer p.m.Unlock()
	defer p.buf.Reset()

	req := &protocol.Message{
		Value: p.buf.Bytes(),
	}
	resp, err := p.c.client.Request(protocol.OpPipeline, req)
	if err != nil {
		return nil, err
	}

	conn := bytes.NewBuffer(resp.Value)
	var responses []PipelineResponse
	var resErr error
	for {
		var pres protocol.Message
		err := pres.Read(conn)
		if err == io.EOF {
			break
		}
		if err != nil {
			resErr = multierror.Append(resErr, err)
			continue
		}
		pr := PipelineResponse{
			Client:   p.c,
			response: pres,
		}
		responses = append(responses, pr)
	}
	return responses, nil
}
