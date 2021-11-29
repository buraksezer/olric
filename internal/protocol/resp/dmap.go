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
	"context"

	"github.com/go-redis/redis/v8"
)

// DM.PUT mydmap mykey "Hello" EX 60 NX

type Put struct {
	DMap  string
	Key   string
	Value []byte
	EX    float64
	PX    int64
	EXAT  float64
	PXAT  int64
	NX    bool
	XX    bool
}

func NewPut(dmap, key string, value []byte) *Put {
	return &Put{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (p *Put) SetEX(ex float64) *Put {
	p.EX = ex
	return p
}

func (p *Put) SetPX(px int64) *Put {
	p.PX = px
	return p
}

func (p *Put) SetEXAT(exat float64) *Put {
	p.EXAT = exat
	return p
}

func (p *Put) SetPXAT(pxat int64) *Put {
	p.PXAT = pxat
	return p
}

func (p *Put) SetNX() *Put {
	p.NX = true
	return p
}

func (p *Put) SetXX() *Put {
	p.XX = true
	return p
}

func (p *Put) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, PutCmd)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Value)

	if p.EX != 0 {
		args = append(args, "EX")
		args = append(args, p.EX)
	}

	if p.PX != 0 {
		args = append(args, "PX")
		args = append(args, p.PX)
	}

	if p.EXAT != 0 {
		args = append(args, "EXAT")
		args = append(args, p.EXAT)
	}

	if p.PXAT != 0 {
		args = append(args, "PXAT")
		args = append(args, p.PXAT)
	}

	if p.NX {
		args = append(args, "NX")
	}

	if p.XX {
		args = append(args, "XX")
	}

	return redis.NewStatusCmd(ctx, args...)
}

type PutReplica struct {
	DMap  string
	Key   string
	Value []byte
}

func NewPutReplica(dmap, key string, value []byte) *PutReplica {
	return &PutReplica{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (p *PutReplica) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, PutReplicaCmd)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Value)
	return redis.NewStatusCmd(ctx, args...)
}

type Get struct {
	DMap string
	Key  string
}

func NewGet(dmap, key string) *Get {
	return &Get{
		DMap: dmap,
		Key:  key,
	}
}

func (g *Get) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, GetCmd)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	return redis.NewStringCmd(ctx, args...)
}

type GetEntry struct {
	Get     *Get
	Replica bool
}

func NewGetEntry(dmap, key string) *GetEntry {
	return &GetEntry{
		Get: NewGet(dmap, key),
	}
}

func (g *GetEntry) SetReplica() *GetEntry {
	g.Replica = true
	return g
}

func (g *GetEntry) Command(ctx context.Context) *redis.StringCmd {
	cmd := g.Get.Command(ctx)
	cmd.Args()[0] = GetEntryCmd
	return cmd
}
