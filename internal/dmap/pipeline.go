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

package dmap

import (
	"bytes"
	"errors"
	"io"

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) extractPipelineMessage(conn io.ReadWriteCloser, response *bytes.Buffer) error {
	buf := bufferPool.Get()
	defer bufferPool.Put(buf)

	_, err := protocol.ReadMessage(conn, buf)
	if err != nil {
		return err
	}
	preq := protocol.NewDMapMessageFromRequest(buf)
	err = preq.Decode()
	// Return an error message in pipelined response.
	if err != nil {
		preq.SetStatus(protocol.StatusErrInternalFailure)
		preq.SetValue([]byte(err.Error()))
		return nil
	}
	f, ok := s.operations[preq.Op]
	if !ok {
		preq.SetStatus(protocol.StatusErrInternalFailure)
		preq.SetValue([]byte(neterrors.ErrUnknownOperation.Error()))
		return nil
	}

	presp := preq.Response(response)
	// Call its function to prepare a response.
	f(presp, preq)
	return presp.Encode()
}

func (s *Service) pipelineOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.PipelineMessage)
	conn := protocol.NewBytesToConn(req.Value())
	response := bufferPool.Get()
	defer bufferPool.Put(response)

	// Decode the pipelined messages into an in-memory buffer.
	for {
		err := s.extractPipelineMessage(conn, response)
		if errors.Is(err, io.EOF) {
			// It's done. The last message has been read.
			break
		}
		if err != nil {
			neterrors.ErrorResponse(w, err)
			return
		}
	}

	// Create a success response and assign pipelined responses as value.
	w.SetStatus(protocol.StatusOK)
	w.SetValue(response.Bytes())
}
