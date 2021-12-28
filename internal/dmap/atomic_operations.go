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
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/tidwall/redcon"
)

func (s *Service) incrDecrCommon(cmd, dmap, key string, delta int) (int, error) {
	dm, err := s.getOrCreateDMap(dmap)
	if err != nil {
		return 0, err
	}

	e := newEnv()
	e.dmap = dm.name
	e.key = key
	return dm.atomicIncrDecr(cmd, e, delta)
}

func (s *Service) incrCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	incrCmd, err := resp.ParseIncrCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	latest, err := s.incrDecrCommon(resp.IncrCmd, incrCmd.DMap, incrCmd.Key, incrCmd.Delta)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteInt(latest)
}

func (s *Service) decrCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	decrCmd, err := resp.ParseDecrCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	latest, err := s.incrDecrCommon(resp.DecrCmd, decrCmd.DMap, decrCmd.Key, decrCmd.Delta)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteInt(latest)
}

func (s *Service) getPutCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	getPutCmd, err := resp.ParseGetPutCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	dm, err := s.getOrCreateDMap(getPutCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	e := newEnv()
	e.dmap = getPutCmd.DMap
	e.key = getPutCmd.Key
	e.value = getPutCmd.Value
	old, err := dm.getPut(e)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	if old == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(old.Value())
}
