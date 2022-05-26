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
)

type ResolverCommands struct {
	Commit string
}

var Resolver = &ResolverCommands{
	Commit: "resolver.commit",
}

type ResolverCommit struct {
	Body string
}

func NewResolverCommit(body string) *ResolverCommit {
	return &ResolverCommit{
		Body: body,
	}
}

func (c *ResolverCommit) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, Resolver.Commit)
	args = append(args, c.Body)
	return redis.NewStatusCmd(ctx, args...)
}

func ParseResolverCommit(cmd redcon.Command) (*ResolverCommit, error) {
	if len(cmd.Args) != 2 {
		return nil, errWrongNumber(cmd.Args)
	}

	body := util.BytesToString(cmd.Args[1])
	c := NewResolverCommit(body)
	return c, nil
}
