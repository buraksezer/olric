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
	"context"
	"sync"

	"github.com/buraksezer/olric/internal/protocol"
)

type Tx struct {
	readVersion   uint32
	commitVersion uint32
	zm            *ZMap
	mtx           sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
}

func (s *Service) getReadVersion() (uint32, error) {
	rc := s.client.Get(s.config.Cluster.Sequencer.Addr)
	grvCmd := protocol.NewSequencerReadVersion().Command(s.ctx)
	err := rc.Process(s.ctx, grvCmd)
	if err != nil {
		return 0, err
	}
	raw, err := grvCmd.Uint64()
	if err != nil {
		return 0, err
	}
	return uint32(raw), nil
}

func (z *ZMap) Tx() (*Tx, error) {
	// 1- Get read version from sequencer
	// 2- Receive commands: Put, Get, etc...
	// 3- Commit
	// 4- Get commit version from sequencer
	// 5- Send all these things to the resolver
	// 6- Send mutations to the transaction-log server
	// 7- Pull changes from the transaction log server

	readVersion, err := z.service.getReadVersion()
	if err != nil {
		return nil, err
	}
	return &Tx{
		readVersion: readVersion,
		zm:          z,
	}, nil
}

func (t *Tx) Commit() error {
	return nil
}
