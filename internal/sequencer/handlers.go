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

func (s *Sequencer) getCommitVersionHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseSequencerGetCommitVersion(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	s.mtx.Lock()
	s.commitVersion++
	commitVersion := s.commitVersion
	s.mtx.Unlock()

	conn.WriteInt64(commitVersion)
}

func (s *Sequencer) getReadVersionHandler(conn redcon.Conn, cmd redcon.Command) {
	_, err := protocol.ParseSequencerGetReadVersion(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	s.mtx.Lock()
	readVersion := s.readVersion
	s.mtx.Unlock()

	conn.WriteInt64(readVersion)
}

func (s *Sequencer) updateReadVersionHandler(conn redcon.Conn, cmd redcon.Command) {
	updateReadVersionCmd, err := protocol.ParseSequencerUpdateReadVersion(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	s.mtx.Lock()

	if s.readVersion >= updateReadVersionCmd.CommitVersion {
		s.mtx.Unlock()
		conn.WriteString(protocol.StatusOK)
		return
	}

	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(updateReadVersionCmd.CommitVersion))
	err = s.pebble.Set(ReadVersionKey, data, &pebble.WriteOptions{
		Sync: true,
	})
	if err != nil {
		s.mtx.Unlock()
		protocol.WriteError(conn, err)
		return
	}
	// Latest known commit version
	s.readVersion = updateReadVersionCmd.CommitVersion
	s.mtx.Unlock()

	conn.WriteString(protocol.StatusOK)
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
