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
package client

import (
	"bytes"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"io"
	"sync"
	"time"

	"github.com/buraksezer/olric"

	"github.com/buraksezer/olric/internal/protocol"
)

var ErrInternalServerError = errors.New("internal server error")

type Pipeline struct {
	c          *Client
	m          sync.Mutex
	buf        *bytes.Buffer
	serializer olric.Serializer
}

func (c *Client) NewPipeline() *Pipeline {
	return &Pipeline{
		c:   c,
		buf: new(bytes.Buffer),
	}
}

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

type PipelineResponse struct {
	serializer olric.Serializer
	response   protocol.Message
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
	case pr.response.Op == protocol.OpDecr:
		return "Decr"
	case pr.response.Op == protocol.OpGetPut:
		return "GetPut"
	case pr.response.Op == protocol.OpLockWithTimeout:
		return "LockWithTimeout"
	case pr.response.Op == protocol.OpUnlock:
		return "Unlock"
	case pr.response.Op == protocol.OpDestroy:
		return "Destroy"
	default:
		return "unknown"

	}
	/*
		OpPutEx
		OpGet
		OpDelete
		OpDestroy
		OpLockWithTimeout
		OpUnlock
		OpIncr
		OpDecr
		OpGetPut
	 */
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
			serializer: p.c.serializer,
			response:   pres,
		}
		responses = append(responses, pr)
	}
	return responses, nil
}
