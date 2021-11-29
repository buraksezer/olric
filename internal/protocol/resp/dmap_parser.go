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
	"errors"
	"strconv"
	"strings"

	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

func ParsePutCommand(cmd redcon.Command) (*Put, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPut(
		util.BytesToString(cmd.Args[1]), // DMap
		util.BytesToString(cmd.Args[2]), // Key
		cmd.Args[3],                     // Value
	)

	args := cmd.Args[4:]
	for len(args) > 0 {
		switch arg := strings.ToUpper(util.BytesToString(args[0])); arg {
		case "NX":
			p.SetNX()
			args = args[1:]
			continue
		case "XX":
			p.SetXX()
			args = args[1:]
			continue
		case "PX":
			px, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPX(int64(px))
			args = args[2:]
			continue
		case "EX":
			px, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEx(px)
			args = args[2:]
			continue
		case "EXAT":
			exat, err := strconv.ParseFloat(util.BytesToString(args[1]), 64)
			if err != nil {
				return nil, err
			}
			p.SetEXAT(exat)
			args = args[2:]
			continue
		case "PXAT":
			pxat, err := strconv.ParseInt(util.BytesToString(args[1]), 10, 64)
			if err != nil {
				return nil, err
			}
			p.SetPXAT(pxat)
			args = args[2:]
			continue
		default:
			return nil, errors.New("syntax error")
		}
	}

	return p, nil
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

type PutReplicaCommand struct {
	DMap  string
	Key   string
	Value []byte
}

func ParsePutReplicaCommand(cmd redcon.Command) (*PutReplicaCommand, error) {
	if len(cmd.Args) < 4 {
		return nil, errWrongNumber(cmd.Args)
	}

	return &PutReplicaCommand{
		DMap:  util.BytesToString(cmd.Args[1]),
		Key:   util.BytesToString(cmd.Args[2]),
		Value: cmd.Args[3],
	}, nil
}
