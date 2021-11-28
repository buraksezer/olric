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

package resp

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func WriteError(conn redcon.Conn, err error) {
	conn.WriteError(fmt.Sprintf("ERR %s", err.Error()))
}

func errWrongNumber(args [][]byte) error {
	sb := strings.Builder{}
	for {
		arg := args[0]
		sb.Write(arg)
		args = args[1:]
		if len(args) == 0 {
			break
		}
		sb.WriteByte(0x20)
	}
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(sb.String()))
}
