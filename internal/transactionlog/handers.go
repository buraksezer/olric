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

package transactionlog

import (
	"encoding/binary"
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/cockroachdb/pebble"
	"github.com/tidwall/redcon"
)

func (t *TransactionLog) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
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

func (t *TransactionLog) transactionLogAddHandler(conn redcon.Conn, cmd redcon.Command) {
	tsAddCmd, err := protocol.ParseTransactionLogAdd(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(tsAddCmd.CommitVersion))
	err = t.wal.Set(key, tsAddCmd.Data, &pebble.WriteOptions{Sync: true})
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	conn.WriteString(protocol.StatusOK)
}

func (t *TransactionLog) transactionLogGetHandler(conn redcon.Conn, cmd redcon.Command) {
	tsGetCmd, err := protocol.ParseTransactionLogGet(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(tsGetCmd.ReadVersion))
	data, closer, err := t.wal.Get(key)
	if err == pebble.ErrNotFound {
		err = ErrTransactionNotFound
	}
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	// TODO: Handle error
	defer closer.Close()
	conn.WriteBulk(data)
}
