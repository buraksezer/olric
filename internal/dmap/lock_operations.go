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

	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/pkg/neterrors"
)

func (s *Service) lockOperationCommon(w, r protocol.EncodeDecoder,
	f func(dm *DMap, r protocol.EncodeDecoder) (*LockContext, error)) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	ctx, err := f(dm, r)
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(ctx.token)
}

func (s *Service) lockWithTimeoutOperation(w, r protocol.EncodeDecoder) {
	s.lockOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) (*LockContext, error) {
		req := r.(*protocol.DMapMessage)
		timeout := req.Extra().(protocol.LockWithTimeoutExtra).Timeout
		deadline := req.Extra().(protocol.LockWithTimeoutExtra).Deadline
		return dm.lockKey(protocol.OpPutIfEx, req.Key(), time.Duration(timeout), time.Duration(deadline))
	})
}

func (s *Service) lockOperation(w, r protocol.EncodeDecoder) {
	s.lockOperationCommon(w, r, func(dm *DMap, r protocol.EncodeDecoder) (*LockContext, error) {
		req := r.(*protocol.DMapMessage)
		deadline := req.Extra().(protocol.LockExtra).Deadline
		return dm.lockKey(protocol.OpPutIf, req.Key(), nilTimeout, time.Duration(deadline))
	})
}

func (s *Service) unlockOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	err = dm.unlock(req.Key(), req.Value())
	if err != nil {
		neterrors.ErrorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
