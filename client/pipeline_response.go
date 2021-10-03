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
	"github.com/buraksezer/olric/internal/kvstore/entry"
	"github.com/buraksezer/olric/internal/protocol"
)

// PipelineResponse implements response readers for pipelined requests.
type PipelineResponse struct {
	*Client
	response protocol.EncodeDecoder
}

// Operation returns the current operation name.
func (pr *PipelineResponse) Operation() string {
	switch {
	case pr.response.OpCode() == protocol.OpPut:
		return "Put"
	case pr.response.OpCode() == protocol.OpPutIf:
		return "PutIf"
	case pr.response.OpCode() == protocol.OpPutIfEx:
		return "PutIfEx"
	case pr.response.OpCode() == protocol.OpGet:
		return "Get"
	case pr.response.OpCode() == protocol.OpPutEx:
		return "PutEx"
	case pr.response.OpCode() == protocol.OpDelete:
		return "Delete"
	case pr.response.OpCode() == protocol.OpIncr:
		return "Incr"
	case pr.response.OpCode() == protocol.OpDecr:
		return "Decr"
	case pr.response.OpCode() == protocol.OpGetPut:
		return "GetPut"
	case pr.response.OpCode() == protocol.OpLockWithTimeout:
		return "LockWithTimeout"
	case pr.response.OpCode() == protocol.OpUnlock:
		return "Unlock"
	case pr.response.OpCode() == protocol.OpDestroy:
		return "Destroy"
	default:
		return "unknown"
	}
}

// Get returns the value for the requested key. It returns ErrKeyNotFound if the DB does not contains the key.
// It's thread-safe. It is safe to modify the contents of the returned value.
// It is safe to modify the contents of the argument after Get returns.
func (pr *PipelineResponse) Get() (interface{}, error) {
	if err := checkStatusCode(pr.response); err != nil {
		return nil, err
	}
	// TODO: Currently we cannot use the common interface for storage entries
	// Because, we don't know what storage engine or entry format in use for this
	// entry. This info should be carried in the protocol. For 0.4.x, it's hard to
	// fix. We need to fix this in a new OBP revision.
	entry := entry.New()
	entry.Decode(pr.response.Value())
	return pr.unmarshalValue(entry.Value())
}

// Put sets the value for the requested key. It overwrites any previous value for that key and
// it's thread-safe. It is safe to modify the contents of the arguments after Put returns but not before.
func (pr *PipelineResponse) Put() error {
	return checkStatusCode(pr.response)
}

// PutEx sets the value for the given key with TTL. It overwrites any previous value for that key.
// It's thread-safe. It is safe to modify the contents of the arguments after Put returns but not before.
func (pr *PipelineResponse) PutEx() error {
	return checkStatusCode(pr.response)
}

// Delete deletes the value for the given key. Delete will not return error if key doesn't exist.
// It's thread-safe. It is safe to modify the contents of the argument after Delete returns.
func (pr *PipelineResponse) Delete() error {
	return checkStatusCode(pr.response)
}

// Incr atomically increments key by delta. The return value is the new value after being incremented or an error.
func (pr *PipelineResponse) Incr() (int, error) {
	return pr.processIncrDecrResponse(pr.response)
}

// Decr atomically decrements key by delta. The return value is the new value after being decremented or an error.
func (pr *PipelineResponse) Decr() (int, error) {
	return pr.processIncrDecrResponse(pr.response)
}

// GetPut atomically sets key to value and returns the old value stored at key.
func (pr *PipelineResponse) GetPut() (interface{}, error) {
	return pr.processGetPutResponse(pr.response)
}

// Destroy flushes the given DMap on the cluster. You should know that there is no global lock on DMaps.
// So if you call Put/PutEx and Destroy methods concurrently on the cluster, Put/PutEx calls may set
// new values to the DMap.
func (pr *PipelineResponse) Destroy() error {
	return checkStatusCode(pr.response)
}

// Expire updates the expiry for the given key. It returns ErrKeyNotFound if the
// DB does not contains the key. It's thread-safe.
func (pr *PipelineResponse) Expire() error {
	return checkStatusCode(pr.response)
}

func (pr *PipelineResponse) PutIf() error {
	return checkStatusCode(pr.response)
}

func (pr *PipelineResponse) PutIfEx() error {
	return checkStatusCode(pr.response)
}
