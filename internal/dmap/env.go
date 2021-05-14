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
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

type env struct {
	hkey          uint64
	timestamp     int64
	flags         int16
	opcode        protocol.OpCode
	replicaOpcode protocol.OpCode
	dmap          string
	key           string
	value         []byte
	timeout       time.Duration
	kind          partitions.Kind
	fragment      *fragment
}

func newEnv(opcode protocol.OpCode, name, key string, value []byte, timeout time.Duration, flags int16, kind partitions.Kind) *env {
	e := &env{
		opcode:    opcode,
		dmap:      name,
		key:       key,
		value:     value,
		timestamp: time.Now().UnixNano(),
		timeout:   timeout,
		flags:     flags,
		kind:      kind,
	}
	switch {
	case opcode == protocol.OpPut:
		e.replicaOpcode = protocol.OpPutReplica
	case opcode == protocol.OpPutEx:
		e.replicaOpcode = protocol.OpPutExReplica
	case opcode == protocol.OpPutIf:
		e.replicaOpcode = protocol.OpPutIfReplica
	case opcode == protocol.OpPutIfEx:
		e.replicaOpcode = protocol.OpPutIfExReplica
	}
	return e
}

// newEnvFromReq generates a new protocol message from writeop instance.
func newEnvFromReq(r protocol.EncodeDecoder, kind partitions.Kind) *env {
	e := &env{}
	req := r.(*protocol.DMapMessage)
	e.dmap = req.DMap()
	e.key = req.Key()
	e.value = req.Value()
	e.opcode = req.Op
	e.kind = kind
	e.hkey = partitions.HKey(req.DMap(), req.Key())

	// Set opcode for a possible replica operation
	switch e.opcode {
	case protocol.OpPut:
		e.replicaOpcode = protocol.OpPutReplica
	case protocol.OpPutEx:
		e.replicaOpcode = protocol.OpPutExReplica
	case protocol.OpPutIf:
		e.replicaOpcode = protocol.OpPutIfReplica
	case protocol.OpPutIfEx:
		e.replicaOpcode = protocol.OpPutIfExReplica
	}

	// Extract extras
	switch req.Op {
	case protocol.OpPut, protocol.OpPutReplica:
		e.timestamp = req.Extra().(protocol.PutExtra).Timestamp
	case protocol.OpPutEx, protocol.OpPutExReplica:
		e.timestamp = req.Extra().(protocol.PutExExtra).Timestamp
		e.timeout = time.Duration(req.Extra().(protocol.PutExExtra).TTL)
	case protocol.OpPutIf, protocol.OpPutIfReplica:
		e.flags = req.Extra().(protocol.PutIfExtra).Flags
		e.timestamp = req.Extra().(protocol.PutIfExtra).Timestamp
	case protocol.OpPutIfEx, protocol.OpPutIfExReplica:
		e.flags = req.Extra().(protocol.PutIfExExtra).Flags
		e.timestamp = req.Extra().(protocol.PutIfExExtra).Timestamp
		e.timeout = time.Duration(req.Extra().(protocol.PutIfExExtra).TTL)
	case protocol.OpExpire:
		e.timestamp = req.Extra().(protocol.ExpireExtra).Timestamp
		e.timeout = time.Duration(req.Extra().(protocol.ExpireExtra).TTL)
	}
	return e
}

// toReq generates a new protocol message from a writeop.
func (e *env) toReq(opcode protocol.OpCode) *protocol.DMapMessage {
	req := protocol.NewDMapMessage(opcode)
	req.SetDMap(e.dmap)
	req.SetKey(e.key)
	req.SetValue(e.value)

	// Prepare extras
	switch opcode {
	case protocol.OpPut, protocol.OpPutReplica:
		req.SetExtra(protocol.PutExtra{
			Timestamp: e.timestamp,
		})
	case protocol.OpPutEx, protocol.OpPutExReplica:
		req.SetExtra(protocol.PutExExtra{
			TTL:       e.timeout.Nanoseconds(),
			Timestamp: e.timestamp,
		})
	case protocol.OpPutIf, protocol.OpPutIfReplica:
		req.SetExtra(protocol.PutIfExtra{
			Flags:     e.flags,
			Timestamp: e.timestamp,
		})
	case protocol.OpPutIfEx, protocol.OpPutIfExReplica:
		req.SetExtra(protocol.PutIfExExtra{
			Flags:     e.flags,
			Timestamp: e.timestamp,
			TTL:       e.timeout.Nanoseconds(),
		})
	case protocol.OpExpire:
		req.SetExtra(protocol.ExpireExtra{
			Timestamp: e.timestamp,
			TTL:       e.timeout.Nanoseconds(),
		})
	}
	return req
}
