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
)

func (s *Service) RegisterHandlers() {
	s.respServer.ServeMux().HandleFunc(protocol.PutCmd, s.putCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.GetCmd, s.getCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.DelCmd, s.delCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.DelEntryCmd, s.delEntryCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.GetEntryCmd, s.getEntryCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.PutEntryCmd, s.putEntryCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.ExpireCmd, s.expireCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.PExpireCmd, s.pexpireCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.DestroyCmd, s.destroyCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.ScanCmd, s.scanCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.IncrCmd, s.incrCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.DecrCmd, s.decrCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.GetPutCmd, s.getPutCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.LockCmd, s.lockCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.UnlockCmd, s.unlockCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.LockLeaseCmd, s.lockLeaseCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.PLockLeaseCmd, s.plockLeaseCommandHandler)
	s.respServer.ServeMux().HandleFunc(protocol.MoveFragmentCmd, s.moveFragmentCommandHandler)
}
