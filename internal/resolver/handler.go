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

package resolver

import (
	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"
)

type WrappedKey struct {
	Key  string `msgpack:"k"`
	Kind Kind   `msgpack:"kd"`
}

type CommitMessage struct {
	ReadVersion   uint32       `msgpack:"rv"`
	CommitVersion uint32       `msgpack:"cv"`
	Keys          []WrappedKey `msgpack:"ks"`
}

func (r *Resolver) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
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

func (r *Resolver) commitCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	commitCmd, err := protocol.ParseResolverCommit(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	batch := CommitMessage{}
	err = msgpack.Unmarshal(util.StringToBytes(commitCmd.Body), &batch)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	var keys []*Key
	for _, key := range batch.Keys {
		keys = append(keys, NewKey(key.Key, key.Kind))
	}
	err = r.ssi.Commit(batch.ReadVersion, batch.CommitVersion, keys)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}
	conn.WriteString(protocol.StatusOK)
}
