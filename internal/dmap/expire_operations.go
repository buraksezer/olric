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
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/tidwall/redcon"
)

func (s *Service) expireCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	expireCmd, err := resp.ParseExpireCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(expireCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	var kind = partitions.PRIMARY
	if expireCmd.Replica {
		kind = partitions.BACKUP
	}

	e := &env{
		putConfig: &putConfig{},
		dmap:      expireCmd.DMap,
		key:       expireCmd.Key,
		timeout:   time.Duration(expireCmd.Timeout * float64(time.Second)),
		kind:      kind,
	}

	if expireCmd.Replica {
		err = dm.localExpireOnReplica(e)
	} else {
		err = dm.expire(e)
	}
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	conn.WriteInt(1)
}
