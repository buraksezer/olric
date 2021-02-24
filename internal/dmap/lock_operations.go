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

	"github.com/buraksezer/olric/internal/protocol"
)

func (s *Service) exLockWithTimeoutOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	timeout := req.Extra().(protocol.LockWithTimeoutExtra).Timeout
	deadline := req.Extra().(protocol.LockWithTimeoutExtra).Deadline
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
		return
	}
	ctx, err := dm.lockKey(protocol.OpPutIfEx, req.Key(), time.Duration(timeout), time.Duration(deadline))
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(ctx.token)
}

func (s *Service) exLockOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
		return
	}
	deadline := req.Extra().(protocol.LockExtra).Deadline
	ctx, err := dm.lockKey(protocol.OpPutIf, req.Key(), nilTimeout, time.Duration(deadline))
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
	w.SetValue(ctx.token)
}

func (s *Service) exUnlockOperation(w, r protocol.EncodeDecoder) {
	req := r.(*protocol.DMapMessage)
	dm, err := s.getOrCreateDMap(req.DMap())
	if err != nil {
		errorResponse(w, err)
		return
	}
	err = dm.unlock(req.Key(), req.Value())
	if err != nil {
		errorResponse(w, err)
		return
	}
	w.SetStatus(protocol.StatusOK)
}
