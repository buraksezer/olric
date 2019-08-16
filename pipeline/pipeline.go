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

/*Package pipeline implements pipelining for Olric Binary Protocol. It enables to send multiple
commands to the server without waiting for the replies at all, and finally read the replies
in a single step.*/
package pipeline

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"io"
	"sync"
	"time"

	"github.com/buraksezer/olric"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/transport"
)

var ErrInternalServerError = errors.New("internal server error")

type Pipeline struct {
	m          sync.Mutex
	buf        *bytes.Buffer
	client     *transport.Client
	serializer olric.Serializer
}

type Config struct {
	Addr        string
	DialTimeout time.Duration
	KeepAlive   time.Duration
	Serializer  olric.Serializer
}

func New(c *Config) (*Pipeline, error) {
	if c == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if len(c.Addr) == 0 {
		return nil, fmt.Errorf("Addr cannot be empty")
	}
	if c.Serializer == nil {
		c.Serializer = olric.NewGobSerializer()
	}

	cc := &transport.ClientConfig{
		Addrs:       []string{c.Addr},
		DialTimeout: c.DialTimeout,
		KeepAlive:   c.KeepAlive,
	}
	return &Pipeline{
		buf:        new(bytes.Buffer),
		client:     transport.NewClient(cc),
		serializer: c.Serializer,
	}, nil
}

func (p *Pipeline) Put(dmap, key string, value interface{}) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.serializer.Marshal(value)
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

func (p *Pipeline) PutEx(dmap, key string, value interface{}, timeout time.Duration) error {
	p.m.Lock()
	defer p.m.Unlock()

	data, err := p.serializer.Marshal(value)
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

func (p *Pipeline) Get(dmap, key string) error {
	p.m.Lock()
	defer p.m.Unlock()

	m := &protocol.Message{
		Header: protocol.Header{
			Magic: protocol.MagicReq,
			Op:    protocol.OpGet,
		},
		DMap:  dmap,
		Key:   key,
	}
	return m.Write(p.buf)
}

type PipelineResponse struct {
	serializer olric.Serializer
	response protocol.Message
}

func (pr *PipelineResponse) Operation() string {
	switch {
	case pr.response.Op == protocol.OpPut:
		return "Put"
	case pr.response.Op == protocol.OpGet:
		return "Get"
	case pr.response.Op == protocol.OpPutEx:
		return "PutEx"
	case pr.response.Op == protocol.OpDelete:
		return "Delete"
	case pr.response.Op == protocol.OpIncr:
		return "Incr"
	default:
		return "unknown"
	}
}

func (pr *PipelineResponse) Get() (interface{}, error) {
	if pr.response.Status == protocol.StatusKeyNotFound {
		return nil, olric.ErrKeyNotFound
	}
	var value interface{}
	err := pr.serializer.Unmarshal(pr.response.Value, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (pr *PipelineResponse) Put() error {
	if pr.response.Status == protocol.StatusOK {
		return nil
	}
	return errors.Wrap(ErrInternalServerError, string(pr.response.Value))
}

func (p *Pipeline) Flush() ([]PipelineResponse, error) {
	p.m.Lock()
	defer p.m.Unlock()

	req := &protocol.Message{
		Value: p.buf.Bytes(),
	}
	resp, err := p.client.Request(protocol.OpPipeline, req)
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
			serializer: p.serializer,
			response: pres,
		}
		responses = append(responses, pr)
	}
	return responses, nil
}
