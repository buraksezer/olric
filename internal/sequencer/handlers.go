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

package sequencer

import (
	"encoding/binary"
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/cockroachdb/pebble"
	"github.com/tidwall/redcon"
)

func (s *Sequencer) commitVersionHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseSequencerCommitVersion(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	var currentVersion uint32
	s.mtx.Lock()
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, s.currentVersion+1)
	err = s.pebble.Set(LatestVersionKey, data, &pebble.WriteOptions{
		Sync: true,
	})
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	s.currentVersion++
	currentVersion = s.currentVersion
	s.mtx.Unlock()

	conn.WriteInt64(int64(currentVersion))
}

func (s *Sequencer) readVersionHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseSequencerReadVersion(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	s.mtx.Lock()
	currentVersion := s.currentVersion
	s.mtx.Unlock()

	conn.WriteInt64(int64(currentVersion))
}

func (s *Sequencer) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pingCmd, err := protocol.ParsePingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if pingCmd.Message != "" {
		conn.WriteString(pingCmd.Message)
		return
	}
	conn.WriteString(olric.DefaultPingResponse)
}
