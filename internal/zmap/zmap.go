// Copyright 2018-2022 Burak Sezer
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

package zmap

import (
	"path"
	"strconv"

	"github.com/buraksezer/olric/internal/zmap/config"
	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/pebble"
)

type ZMap struct {
	name    string
	datadir string
	db      *pebble.DB
	service *Service
	config  *config.Config
}

func (s *Service) NewZMap(name string, c *config.Config) (*ZMap, error) {
	// Check operation status first:
	//
	// * Checks member count in the cluster, returns ErrClusterQuorum if
	//   the quorum value cannot be satisfied,
	// * Checks bootstrapping status and awaits for a short period before
	//   returning ErrRequest timeout.
	if err := s.rt.CheckMemberCountQuorum(); err != nil {
		return nil, err
	}
	// An Olric node has to be bootstrapped to function properly.
	if err := s.rt.CheckBootstrap(); err != nil {
		return nil, err
	}

	// TODO: Remove the config parameter after merging into master
	s.mtx.Lock()
	defer s.mtx.Unlock()

	zm, ok := s.zmaps[name]
	if ok {
		return zm, nil
	}

	if c == nil {
		c = config.DefaultConfig()
	}

	hashedName := strconv.FormatUint(xxhash.Sum64String(name), 10)
	datadir := path.Join(c.DataDir, hashedName)

	db, err := pebble.Open(datadir, nil)
	if err != nil {
		return nil, err
	}

	zm = &ZMap{
		config:  c,
		datadir: datadir,
		db:      db,
		name:    name,
		service: s,
	}
	s.zmaps[name] = zm
	return zm, nil
}

func (z *ZMap) Name() string {
	return z.name
}
