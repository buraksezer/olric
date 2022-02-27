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
)

type Ping struct {
	Message string
}

func NewPing() *Ping {
	return &Ping{}
}

func (p *Ping) SetMessage(m string) *Ping {
	p.Message = m
	return p
}

func (p *Ping) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Generic.Ping)
	if p.Message != "" {
		args = append(args, p.Message)
	}
	return redis.NewStringCmd(ctx, args...)
}

type MoveFragment struct {
	Payload []byte
}

func NewMoveFragment(payload []byte) *MoveFragment {
	return &MoveFragment{
		Payload: payload,
	}
}

func (m *MoveFragment) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, Internal.MoveFragment)
	args = append(args, m.Payload)
	return redis.NewStatusCmd(ctx, args...)
}

type UpdateRouting struct {
	Payload       []byte
	CoordinatorID uint64
}

func NewUpdateRouting(payload []byte, coordinatorID uint64) *UpdateRouting {
	return &UpdateRouting{
		Payload:       payload,
		CoordinatorID: coordinatorID,
	}
}

func (u *UpdateRouting) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Internal.UpdateRouting)
	args = append(args, u.Payload)
	args = append(args, u.CoordinatorID)
	return redis.NewStringCmd(ctx, args...)
}

type LengthOfPart struct {
	PartID  uint64
	Replica bool
}

func NewLengthOfPart(partID uint64) *LengthOfPart {
	return &LengthOfPart{
		PartID: partID,
	}
}

func (l *LengthOfPart) SetReplica() *LengthOfPart {
	l.Replica = true
	return l
}

func (l *LengthOfPart) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, Internal.LengthOfPart)
	args = append(args, l.PartID)
	if l.Replica {
		args = append(args, "RC")
	}
	return redis.NewIntCmd(ctx, args...)
}

type Stats struct {
	CollectRuntime bool
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) SetCollectRuntime() *Stats {
	s.CollectRuntime = true
	return s
}

func (s *Stats) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, Generic.Stats)
	if s.CollectRuntime {
		args = append(args, "CR")
	}
	return redis.NewStringCmd(ctx, args...)
}
