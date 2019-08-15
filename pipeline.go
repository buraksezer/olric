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
	"github.com/buraksezer/olric/internal/protocol"
	"io"
)

func (db *Olric) pipelineOperation(req *protocol.Message) *protocol.Message {
	conn := bytes.NewBuffer(req.Value)
	response := &bytes.Buffer{}
	for {
		var preq protocol.Message
		err := preq.Read(conn)
		if err == io.EOF {
			break
		}
		if err != nil {
			err = preq.Error(protocol.StatusInternalServerError, err).Write(response)
			if err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
			continue
		}
		f, err := db.server.GetOperation(preq.Op)
		if err != nil {
			err = preq.Error(protocol.StatusInternalServerError, err).Write(response)
			if err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
			continue
		}

		pres := f(&preq)
		err = pres.Write(response)
		if err != nil {
			return req.Error(protocol.StatusInternalServerError, err)
		}
	}
	resp := req.Success()
	resp.Value = response.Bytes()
	return resp
}