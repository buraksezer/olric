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
	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

type PutCommand struct {
	DMap  string
	Key   string
	Value []byte
}

func ParsePutCommand(cmd redcon.Command) (*PutCommand, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}
	return &PutCommand{
		DMap:  util.BytesToString(cmd.Args[1]),
		Key:   util.BytesToString(cmd.Args[2]),
		Value: cmd.Args[3],
	}, nil
}

type GetCommand struct {
	DMap string
	Key  string
}

func ParseGetCommand(cmd redcon.Command) (*GetCommand, error) {
	if len(cmd.Args) < 3 {
		return nil, errWrongNumber(cmd.Args)
	}
	return &GetCommand{
		DMap: util.BytesToString(cmd.Args[1]),
		Key:  util.BytesToString(cmd.Args[2]),
	}, nil
}
