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

package olric

import (
	"bytes"
	"io"

	"github.com/buraksezer/olric/internal/protocol"
)

func (db *Olric) pipelineOperation(req *protocol.Message) *protocol.Message {
	conn := bytes.NewBuffer(req.Value)
	response := &bytes.Buffer{}
	// Read the pipelined messages into an in-memory buffer.
	for {
		var preq protocol.Message
		err := preq.Read(conn)
		if err == io.EOF {
			// It's done. The last message has been read.
			break
		}

		// Return an error message in pipelined response.
		if err != nil {
			err = preq.Error(protocol.StatusInternalServerError, err).Write(response)
			if err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
			continue
		}
		f, ok := db.operations[preq.Op]
		if !ok {
			err = preq.Error(protocol.StatusInternalServerError, ErrUnknownOperation).Write(response)
			if err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
			continue
		}

		// Call its function to prepare a response.
		pres := f(&preq)
		err = pres.Write(response)
		if err != nil {
			return req.Error(protocol.StatusInternalServerError, err)
		}
	}

	// Create a success response and assign pipelined responses as Value.
	resp := req.Success()
	resp.Value = response.Bytes()
	return resp
}
