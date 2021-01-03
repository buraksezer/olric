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

package dmap

import (
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
)

func (s *Service) exIncrDecrOperation(w, r protocol.EncodeDecoder) {
	var delta interface{}
	req := r.(*protocol.DMapMessage)
	err := s.serializer.Unmarshal(req.Value(), &delta)
	if err != nil {
		errorResponse(w, err)
		return
	}

	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		dm, err = s.NewDMap(req.DMap())
		if err != nil {
			errorResponse(w, err)
			return
		}
	}
	if err != nil {
		errorResponse(w, err)
		return
	}
	e := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          req.DMap(),
		key:           req.Key(),
		timestamp:     time.Now().UnixNano(),
		kind:          partitions.PRIMARY,
	}
	newval, err := dm.atomicIncrDecr(req.Op, e, delta.(int))
	if err != nil {
		errorResponse(w, err)
		return
	}

	value, err := s.serializer.Marshal(newval)
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (s *Service) exGetPutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)

	dm, err := s.LoadDMap(req.DMap())
	if err == ErrDMapNotFound {
		dm, err = s.NewDMap(req.DMap())
		if err != nil {
			errorResponse(w, err)
			return
		}
	}
	if err != nil {
		errorResponse(w, err)
		return
	}

	e := &env{
		opcode:        protocol.OpPut,
		replicaOpcode: protocol.OpPutReplica,
		dmap:          req.DMap(),
		key:           req.Key(),
		value:         req.Value(),
		timestamp:     time.Now().UnixNano(),
		kind:          partitions.PRIMARY,
	}
	oldval, err := dm.getPut(e)
	if err != nil {
		errorResponse(w, err)
		return
	}
	if oldval != nil {
		w.SetValue(oldval)
	}
	w.SetStatus(protocol.StatusOK)
}
