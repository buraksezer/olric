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
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
	"github.com/buraksezer/olric/query"
	"github.com/vmihailenco/msgpack"
)

func (s *Service) queryOperationCommon(w, r protocol.EncodeDecoder,
	f func(dm *DMap, q query.M, r protocol.EncodeDecoder) (interface{}, error)) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err == ErrDMapNotFound {
		// No need to create a new DMap here.
		w.SetStatus(protocol.StatusOK)
		return
	}
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	q, err := query.FromByte(req.Value())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}

	result, err := f(dm, q, r)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	value, err := msgpack.Marshal(&result)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(value)
}

func (s *Service) localQueryOperation(w, r protocol.EncodeDecoder) {
	s.queryOperationCommon(w, r,
		func(dm *DMap, q query.M, r protocol.EncodeDecoder) (interface{}, error) {
			req := r.(*protocol.DMapMessage)
			partID := req.Extra().(protocol.LocalQueryExtra).PartID
			return dm.runLocalQuery(partID, q)
		})
}

func (s *Service) queryOperation(w, r protocol.EncodeDecoder) {
	s.queryOperationCommon(w, r,
		func(dm *DMap, q query.M, r protocol.EncodeDecoder) (interface{}, error) {
			req := r.(*protocol.DMapMessage)
			c, err := dm.Query(q)
			if err != nil {
				return nil, err
			}
			defer c.Close()

			partID := req.Extra().(protocol.QueryExtra).PartID
			if partID >= s.config.PartitionCount {
				return nil, ErrEndOfQuery
			}
			responses, err := c.runQueryOnOwners(partID)
			if err != nil {
				return nil, ErrEndOfQuery
			}

			data := make(QueryResponse)
			for _, response := range responses {
				data[response.Key()] = response.Value()
			}
			return data, nil
		})
}
