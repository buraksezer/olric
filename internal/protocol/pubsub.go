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

package protocol

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type Publish struct {
	Channel string
	Message string
}

func NewPublish(channel, message string) *Publish {
	return &Publish{
		Channel: channel,
		Message: message,
	}
}

func (p *Publish) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, PubSub.Publish)
	args = append(args, p.Channel)
	args = append(args, p.Message)
	return redis.NewIntCmd(ctx, args...)
}

type Subscribe struct {
	Channels []string
}

func NewSubscribe(channels ...string) *Subscribe {
	return &Subscribe{
		Channels: channels,
	}
}

func (s *Subscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.Subscribe)
	for _, channel := range s.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PSubscribe struct {
	Patterns []string
}

func NewPSubscribe(patterns ...string) *PSubscribe {
	return &PSubscribe{
		Patterns: patterns,
	}
}

func (s *PSubscribe) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.Subscribe)
	for _, channel := range s.Patterns {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PubSubChannels struct {
	Pattern string
}

func NewPubSubChannels() *PubSubChannels {
	return &PubSubChannels{}
}

func (ps *PubSubChannels) SetPattern(pattern string) *PubSubChannels {
	ps.Pattern = pattern
	return ps
}

func (ps *PubSubChannels) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubChannels)
	if ps.Pattern != "" {
		args = append(args, ps.Pattern)
	}
	return redis.NewSliceCmd(ctx, args...)
}

type PubSubNumpat struct{}

func NewPubSubNumpat() *PubSubNumpat {
	return &PubSubNumpat{}
}

func (ps *PubSubNumpat) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubChannels)
	return redis.NewIntCmd(ctx, args...)
}

type PubSubNumsub struct {
	Channels []string
}

func NewPubSubNumsub(channels ...string) *PubSubNumsub {
	return &PubSubNumsub{
		Channels: channels,
	}
}

func (ps *PubSubNumsub) Command(ctx context.Context) *redis.SliceCmd {
	var args []interface{}
	args = append(args, PubSub.PubSubNumsub)
	for _, channel := range ps.Channels {
		args = append(args, channel)
	}
	return redis.NewSliceCmd(ctx, args...)
}
