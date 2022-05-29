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
	"github.com/buraksezer/olric/internal/util"
	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
	"strconv"
)

type SequencerCommands struct {
	GetCommitVersion  string
	UpdateReadVersion string
	GetReadVersion    string
}

var Sequencer = &SequencerCommands{
	GetCommitVersion:  "sequencer.getcommitversion",
	UpdateReadVersion: "sequencer.updatereadversion",
	GetReadVersion:    "sequencer.getreadversion",
}

type SequencerGetCommitVersion struct{}

func NewSequencerGetCommitVersion() *SequencerGetCommitVersion {
	return &SequencerGetCommitVersion{}
}

func (c *SequencerGetCommitVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Sequencer.GetCommitVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseSequencerGetCommitVersion(cmd redcon.Command) (*SequencerGetCommitVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewSequencerGetCommitVersion()
	return c, nil
}

type SequencerGetReadVersion struct{}

func NewSequencerGetReadVersion() *SequencerGetReadVersion {
	return &SequencerGetReadVersion{}
}

func (c *SequencerGetReadVersion) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Sequencer.GetReadVersion)
	return redis.NewIntCmd(ctx, args...)
}

func ParseSequencerGetReadVersion(cmd redcon.Command) (*SequencerGetReadVersion, error) {
	if len(cmd.Args) > 1 {
		return nil, errWrongNumber(cmd.Args)
	}

	c := NewSequencerGetReadVersion()
	return c, nil
}

type SequencerUpdateReadVersion struct {
	CommitVersion int64
}

func NewSequencerUpdateReadVersion(commitVersion int64) *SequencerUpdateReadVersion {
	return &SequencerUpdateReadVersion{CommitVersion: commitVersion}
}

func (c *SequencerUpdateReadVersion) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, Sequencer.UpdateReadVersion)
	args = append(args, c.CommitVersion)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseSequencerUpdateReadVersion(cmd redcon.Command) (*SequencerUpdateReadVersion, error) {
	if len(cmd.Args) > 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	commitVersion, err := strconv.ParseInt(util.BytesToString(cmd.Args[1]), 10, 64)
	if err != nil {
		return nil, err
	}
	c := NewSequencerUpdateReadVersion(commitVersion)
	return c, nil
}
