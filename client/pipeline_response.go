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

import "github.com/buraksezer/olric/internal/protocol"

type PipelineResponse struct {
	*Client
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
}

func (pr *PipelineResponse) Get() (interface{}, error) {
	return pr.processGetResponse(&pr.response)
}

func (pr *PipelineResponse) Put() error {
	return checkStatusCode(&pr.response)
}

func (pr *PipelineResponse) PutEx() error {
	return checkStatusCode(&pr.response)
}

func (pr *PipelineResponse) Delete() error {
	return checkStatusCode(&pr.response)
}

func (pr *PipelineResponse) Incr() (int, error) {
	return pr.processIncrDecrResponse(&pr.response)
}

func (pr *PipelineResponse) Decr() (int, error) {
	return pr.processIncrDecrResponse(&pr.response)
}

func (pr *PipelineResponse) GetPut() (interface{}, error) {
	return pr.processGetPutResponse(&pr.response)
}

func (pr *PipelineResponse) Destroy() error {
	return checkStatusCode(&pr.response)
}

func (pr *PipelineResponse) LockWithTimeout() error {
	return checkStatusCode(&pr.response)
}

func (pr *PipelineResponse) Unlock() error {
	return checkStatusCode(&pr.response)
}
