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
	"github.com/buraksezer/olric/query"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack"
)

func (s *Service) queryOnCluster(dm *DMap, q query.M, partID uint64) (interface{}, error) {
	c, err := dm.Query(q)
	if err != nil {
		return nil, err
	}
	defer c.Close()

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
}

func (s *Service) queryCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	queryCmd, err := resp.ParseQueryCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(queryCmd.DMap)
	if err == ErrDMapNotFound {
		// No need to create a new DMap here.
		conn.WriteString(resp.StatusOK)
		return
	}
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	q, err := query.FromByte(queryCmd.Query)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	var result interface{}
	if queryCmd.Local {
		result, err = dm.runLocalQuery(queryCmd.PartID, q)
	} else {
		result, err = s.queryOnCluster(dm, q, queryCmd.PartID)
	}

	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	value, err := msgpack.Marshal(&result)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	conn.WriteBulk(value)
}
