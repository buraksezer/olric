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

/*
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
}*/

func (s *Service) scanCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	scanCmd, err := resp.ParseScanCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	_, err = s.getOrCreateDMap(scanCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}
	/*for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		part := dm.s.primary.PartitionByID(partID)
		f, err := dm.loadFragment(part)
		if err != nil {
			resp.WriteError(conn, err)
			return
		}
		f.Lock()
		cursor, err := f.storage.(*kvstore.KVStore).Scan(scanCmd.Cursor, scanCmd.Count, func(_ uint64, e storage.Entry) bool {

		})
		if err != nil {
			resp.WriteError(conn, err)
			return
		}
	}*/
	conn.WriteArray(2)
	conn.WriteBulkString("12312")
	conn.WriteArray(2)
	conn.WriteBulkString("burak")
	conn.WriteBulkString("sezer")
}
