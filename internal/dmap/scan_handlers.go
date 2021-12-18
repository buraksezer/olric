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
	"strconv"

	"github.com/buraksezer/olric/internal/kvstore"
	"github.com/buraksezer/olric/internal/protocol/resp"
	"github.com/buraksezer/olric/pkg/storage"
	"github.com/tidwall/redcon"
)

func (dm *DMap) scanOnFragment(f *fragment, cursor uint64, count int) ([]string, uint64, error) {
	f.Lock()
	defer f.Unlock()

	var items []string
	cursor, err := f.storage.(*kvstore.KVStore).Scan(cursor, count, func(e storage.Entry) bool {
		items = append(items, e.Key())
		return true
	})
	if err != nil {
		return nil, 0, err
	}

	return items, cursor, nil
}

func (dm *DMap) scanOnCluster(partID, cursor uint64, sc *scanConfig) ([]string, uint64, error) {
	part := dm.s.primary.PartitionByID(partID)
	f, err := dm.loadFragment(part)
	if err == errFragmentNotFound {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	return dm.scanOnFragment(f, cursor, sc.Count)
}

func (dm *DMap) scan(partID, cursor uint64, sc *scanConfig) ([]string, uint64, error) {
	member := dm.s.primary.PartitionByID(partID).Owner()
	if member.CompareByName(dm.s.rt.This()) {
		items, cursor, err := dm.scanOnCluster(partID, cursor, sc)
		if err != nil {
			return nil, 0, err
		}
		return items, cursor, nil
	}

	// Redirect to the partition owner
	scan := resp.NewScan(partID, dm.name, cursor)
	if sc.HasMatch {
		scan.SetMatch(scan.Match)
	}
	if sc.HashCount {
		scan.SetCount(scan.Count)
	}
	cmd := scan.Command(dm.s.ctx)

	rc := dm.s.respClient.Get(member.String())
	err := rc.Process(dm.s.ctx, cmd)
	if err != nil {
		return nil, 0, resp.ConvertError(err)
	}
	err = cmd.Err()
	if err != nil {
		return nil, 0, resp.ConvertError(err)
	}
	return cmd.Result()
}

type scanConfig struct {
	HashCount bool
	Count     int
	HasMatch  bool
	Match     string
}

type ScanOption func(*scanConfig)

func Count(c int) ScanOption {
	return func(cfg *scanConfig) {
		cfg.HashCount = true
		cfg.Count = c
	}
}

func Match(s string) ScanOption {
	return func(cfg *scanConfig) {
		cfg.HasMatch = true
		cfg.Match = s
	}
}

func (s *Service) scanCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	scanCmd, err := resp.ParseScanCommand(cmd)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	dm, err := s.getOrCreateDMap(scanCmd.DMap)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	var sc scanConfig
	var options []ScanOption
	options = append(options, Count(scanCmd.Count))
	options = append(options, Match(scanCmd.Match))
	for _, opt := range options {
		opt(&sc)
	}

	result, cursor, err := dm.scan(scanCmd.PartID, scanCmd.Cursor, &sc)
	if err != nil {
		resp.WriteError(conn, err)
		return
	}

	conn.WriteArray(2)
	conn.WriteBulkString(strconv.FormatUint(cursor, 10))
	conn.WriteArray(len(result))
	for _, i := range result {
		conn.WriteBulkString(i)
	}
}
