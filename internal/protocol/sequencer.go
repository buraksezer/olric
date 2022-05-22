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
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
)

type SequencerCommands struct {
	CommitVersion string
	ReadVersion   string
}

var Sequencer = &SequencerCommands{
	CommitVersion: "sequencer.commitversion",
	ReadVersion:   "sequencer.readversion",
}

type SequencerCommitVersion struct{}

func NewSequencerCommitVersion() *SequencerCommitVersion {
	return &SequencerCommitVersion{}
}

func (c *SequencerCommitVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Sequencer.CommitVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseSequencerCommitVersion(cmd redcon.Command) (*SequencerCommitVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewSequencerCommitVersion()
	return c, nil
}

type SequencerReadVersion struct{}

func NewSequencerReadVersion() *SequencerReadVersion {
	return &SequencerReadVersion{}
}

func (c *SequencerReadVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Sequencer.ReadVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseSequencerReadVersion(cmd redcon.Command) (*SequencerReadVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewSequencerReadVersion()
	return c, nil
}
