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

package protocol

import (
	"fmt"
	"strconv"

	"github.com/buraksezer/olric/internal/util"
	"github.com/tidwall/redcon"
)

func ParsePingCommand(cmd redcon.Command) (*Ping, error) {
	if len(cmd.Args) < 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	p := NewPing()
	if len(cmd.Args) == 2 {
		p.SetMessage(util.BytesToString(cmd.Args[1]))
	}
	return p, nil
}

func ParseMoveFragmentCommand(cmd redcon.Command) (*MoveFragment, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	return NewMoveFragment(cmd.Args[1]), nil
}

func ParseUpdateRoutingCommand(cmd redcon.Command) (*UpdateRouting, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}
	coordinatorID, err := strconv.ParseUint(util.BytesToString(cmd.Args[2]), 10, 64)
	if err != nil {
		return nil, err
	}

	return NewUpdateRouting(cmd.Args[1], coordinatorID), nil
}

func ParseLengthOfPartCommand(cmd redcon.Command) (*LengthOfPart, error) {
	if len(cmd.Args) < 2 {
		return nil, errWrongNumber(cmd.Args)
	}
	partID, err := strconv.ParseUint(util.BytesToString(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}

	l := NewLengthOfPart(partID)
	if len(cmd.Args) == 3 {
		arg := util.BytesToString(cmd.Args[2])
		if arg == "RC" {
			l.SetReplica()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return l, nil
}

func ParseStatsCommand(cmd redcon.Command) (*Stats, error) {
	if len(cmd.Args) < 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	s := NewStats()
	if len(cmd.Args) == 2 {
		arg := util.BytesToString(cmd.Args[1])
		if arg == "CR" {
			s.SetCollectRuntime()
		} else {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, arg)
		}
	}

	return s, nil
}
