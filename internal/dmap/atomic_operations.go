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
	"fmt"
	"reflect"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func valueToInt(delta interface{}) (int, error) {
	switch value := delta.(type) {
	case int:
		return value, nil
	case int8:
		return int(value), nil
	case int16:
		return int(value), nil
	case int32:
		return int(value), nil
	case int64:
		return int(value), nil
	default:
		return 0, fmt.Errorf("mismatched type: %v", reflect.TypeOf(delta))
	}
}

func (s *Service) incrDecrOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
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
	var delta interface{}
	err = s.serializer.Unmarshal(req.Value(), &delta)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	v, err := valueToInt(delta)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	latest, err := dm.atomicIncrDecr(req.Op, e, v)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	value, err := s.serializer.Marshal(latest)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (s *Service) getPutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
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
	old, err := dm.getPut(e)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	if old != nil {
		w.SetValue(old)
	}
	w.SetStatus(protocol.StatusOK)
}
