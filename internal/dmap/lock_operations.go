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
	"encoding/hex"
	"time"

	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/tidwall/redcon"
)

func (s *Service) unlockCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	unlockCmd, err := resp.ParseUnlockCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(unlockCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	token, err := hex.DecodeString(unlockCmd.Token)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	err = dm.unlock(unlockCmd.Key, token)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	conn.WriteString(resp.StatusOK)
}

func (s *Service) lockCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	lockCmd, err := resp.ParseLockCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(lockCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	var timeout = nilTimeout
	switch {
	case lockCmd.EX != 0:
		timeout = time.Duration(lockCmd.EX * float64(time.Second))
	case lockCmd.PX != 0:
		timeout = time.Duration(lockCmd.PX * int64(time.Millisecond))
	}

	var deadline = time.Duration(lockCmd.Deadline * float64(time.Second))
	lctx, err := dm.lockKey(lockCmd.Key, timeout, deadline)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	conn.WriteString(hex.EncodeToString(lctx.token))
}

func (s *Service) lockLeaseCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	lockLeaseCmd, err := resp.ParseLockLeaseCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(lockLeaseCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	timeout := time.Duration(lockLeaseCmd.Timeout * float64(time.Second))
	token, err := hex.DecodeString(lockLeaseCmd.Token)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	err = dm.lease(lockLeaseCmd.Key, token, timeout)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteString(resp.StatusOK)
}
