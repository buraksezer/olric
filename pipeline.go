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

package olric

import (
	"bytes"
	"io"

	"github.com/buraksezer/olric/internal/protocol"
)

func (db *Olric) extractPipelineMessage(conn io.ReadWriteCloser, response *bytes.Buffer) error {
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
		preq.SetStatus(protocol.StatusInternalServerError)
		preq.SetValue([]byte(err.Error()))
		return nil
	}
	f, ok := db.operations[preq.Op]
	if !ok {
		preq.SetStatus(protocol.StatusInternalServerError)
		preq.SetValue([]byte(ErrUnknownOperation.Error()))
		return nil
	}

	presp := preq.Response(response)
	// Call its function to prepare a response.
	f(presp, preq)
	return presp.Encode()
}

func (db *Olric) pipelineOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.PipelineMessage)
	conn := protocol.NewBytesToConn(req.Value())
	response := bufferPool.Get()
	defer bufferPool.Put(response)

	// Decode the pipelined messages into an in-memory buffer.
	for {
		err := db.extractPipelineMessage(conn, response)
		if err == io.EOF {
			// It's done. The last message has been read.
			break
		}
		if err != nil {
			db.errorResponse(w, err)
			return
		}
	}

	// Create a success response and assign pipelined responses as value.
	w.SetStatus(protocol.StatusOK)
	w.SetValue(response.Bytes())
}
