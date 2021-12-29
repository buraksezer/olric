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

package olric

import (
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/tidwall/redcon"
)

// Ping sends a dummy protocol message to the given host. This is useful to
// measure RTT between hosts. It also can be used as aliveness check.
func (db *Olric) Ping(addr string) error {
	cmd := protocol.NewPing().Command(db.ctx)
	rc := db.respClient.Get(addr)
	err := rc.Process(db.ctx, cmd)
	if err != nil {
		return err
	}
	return cmd.Err()
}

func (db *Olric) pingCommandHandler(conn redcon.Conn, cmd redcon.Command) {
	pingCmd, err := protocol.ParsePingCommand(cmd)
	if err != nil {
		protocol.WriteError(conn, err)
		return
	}

	if pingCmd.Message != "" {
		conn.WriteString(pingCmd.Message)
		return
	}
	conn.WriteString("PONG")
}
