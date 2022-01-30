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
	s.server.ServeMux().HandleFunc(protocol.PutCmd, s.putCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.GetCmd, s.getCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DelCmd, s.delCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DelEntryCmd, s.delEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.GetEntryCmd, s.getEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PutEntryCmd, s.putEntryCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.ExpireCmd, s.expireCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PExpireCmd, s.pexpireCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DestroyCmd, s.destroyCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.ScanCmd, s.scanCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.IncrCmd, s.incrCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.DecrCmd, s.decrCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.GetPutCmd, s.getPutCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.LockCmd, s.lockCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.UnlockCmd, s.unlockCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.LockLeaseCmd, s.lockLeaseCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.PLockLeaseCmd, s.plockLeaseCommandHandler)
	s.server.ServeMux().HandleFunc(protocol.MoveFragmentCmd, s.moveFragmentCommandHandler)
}
