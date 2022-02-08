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
	"time"

	"github.com/go-redis/redis/v8"
)

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
	args = append(args, DMap.Put)
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

type PutEntry struct {
	DMap  string
	Key   string
	Value []byte
}

func NewPutEntry(dmap, key string, value []byte) *PutEntry {
	return &PutEntry{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (p *PutEntry) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PutEntry)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Value)
	return redis.NewStatusCmd(ctx, args...)
}

type Get struct {
	DMap string
	Key  string
	Raw  bool
}

func NewGet(dmap, key string) *Get {
	return &Get{
		DMap: dmap,
		Key:  key,
	}
}

func (g *Get) SetRaw() *Get {
	g.Raw = true
	return g
}

func (g *Get) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.Get)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	if g.Raw {
		args = append(args, "RW")
	}
	return redis.NewStringCmd(ctx, args...)
}

type GetEntry struct {
	DMap    string
	Key     string
	Replica bool
}

func NewGetEntry(dmap, key string) *GetEntry {
	return &GetEntry{
		DMap: dmap,
		Key:  key,
	}
}

func (g *GetEntry) SetReplica() *GetEntry {
	g.Replica = true
	return g
}

func (g *GetEntry) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.GetEntry)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	if g.Replica {
		args = append(args, "RC")
	}
	return redis.NewStringCmd(ctx, args...)
}

type Del struct {
	DMap string
	Key  string
}

func NewDel(dmap, key string) *Del {
	return &Del{
		DMap: dmap,
		Key:  key,
	}
}

func (d *Del) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DMap.Del)
	args = append(args, d.DMap)
	args = append(args, d.Key)
	return redis.NewIntCmd(ctx, args...)
}

type DelEntry struct {
	Del     *Del
	Replica bool
}

func NewDelEntry(dmap, key string) *DelEntry {
	return &DelEntry{
		Del: NewDel(dmap, key),
	}
}

func (d *DelEntry) SetReplica() *DelEntry {
	d.Replica = true
	return d
}

func (d *DelEntry) Command(ctx context.Context) *redis.IntCmd {
	cmd := d.Del.Command(ctx)
	args := cmd.Args()
	args[0] = DMap.DelEntry
	if d.Replica {
		args = append(args, "RC")
	}
	return redis.NewIntCmd(ctx, args...)
}

type PExpire struct {
	DMap         string
	Key          string
	Milliseconds time.Duration
}

func NewPExpire(dmap, key string, milliseconds time.Duration) *PExpire {
	return &PExpire{
		DMap:         dmap,
		Key:          key,
		Milliseconds: milliseconds,
	}
}

func (p *PExpire) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PExpire)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Milliseconds.Milliseconds())
	return redis.NewStatusCmd(ctx, args...)
}

type Expire struct {
	DMap    string
	Key     string
	Seconds time.Duration
}

func NewExpire(dmap, key string, seconds time.Duration) *Expire {
	return &Expire{
		DMap:    dmap,
		Key:     key,
		Seconds: seconds,
	}
}

func (e *Expire) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Expire)
	args = append(args, e.DMap)
	args = append(args, e.Key)
	args = append(args, e.Seconds.Seconds())
	return redis.NewStatusCmd(ctx, args...)
}

type Destroy struct {
	DMap  string
	Local bool
}

func NewDestroy(dmap string) *Destroy {
	return &Destroy{
		DMap: dmap,
	}
}

func (d *Destroy) SetLocal() *Destroy {
	d.Local = true
	return d
}

func (d *Destroy) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Destroy)
	args = append(args, d.DMap)
	if d.Local {
		args = append(args, "LC")
	}
	return redis.NewStatusCmd(ctx, args...)
}

type Scan struct {
	PartID  uint64
	DMap    string
	Cursor  uint64
	Count   int
	Match   string
	Replica bool
}

func NewScan(partID uint64, dmap string, cursor uint64) *Scan {
	return &Scan{
		PartID: partID,
		DMap:   dmap,
		Cursor: cursor,
	}
}

func (s *Scan) SetMatch(match string) *Scan {
	s.Match = match
	return s
}

func (s *Scan) SetCount(count int) *Scan {
	s.Count = count
	return s
}

func (s *Scan) SetReplica() *Scan {
	s.Replica = true
	return s
}

func (s *Scan) Command(ctx context.Context) *redis.ScanCmd {
	var args []interface{}
	args = append(args, DMap.Scan)
	args = append(args, s.PartID)
	args = append(args, s.DMap)
	args = append(args, s.Cursor)
	if s.Match != "" {
		args = append(args, "MATCH")
		args = append(args, s.Match)
	}
	if s.Count != 0 {
		args = append(args, "COUNT")
		args = append(args, s.Count)
	}
	if s.Replica {
		args = append(args, "RC")
	}
	return redis.NewScanCmd(ctx, nil, args...)
}

type Incr struct {
	DMap  string
	Key   string
	Delta int
}

func NewIncr(dmap, key string, delta int) *Incr {
	return &Incr{
		DMap:  dmap,
		Key:   key,
		Delta: delta,
	}
}

func (i *Incr) Command(ctx context.Context) *redis.IntCmd {
	var args []interface{}
	args = append(args, DMap.Incr)
	args = append(args, i.DMap)
	args = append(args, i.Key)
	args = append(args, i.Delta)
	return redis.NewIntCmd(ctx, args...)
}

type Decr struct {
	*Incr
}

func NewDecr(dmap, key string, delta int) *Decr {
	return &Decr{
		NewIncr(dmap, key, delta),
	}
}

func (d *Decr) Command(ctx context.Context) *redis.IntCmd {
	cmd := d.Incr.Command(ctx)
	cmd.Args()[0] = DMap.Decr
	return cmd
}

type GetPut struct {
	DMap  string
	Key   string
	Value []byte
}

func NewGetPut(dmap, key string, value []byte) *GetPut {
	return &GetPut{
		DMap:  dmap,
		Key:   key,
		Value: value,
	}
}

func (g *GetPut) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.GetPut)
	args = append(args, g.DMap)
	args = append(args, g.Key)
	args = append(args, g.Value)
	return redis.NewStringCmd(ctx, args...)
}

// TODO: Add PLock

type Lock struct {
	DMap     string
	Key      string
	Deadline float64
	EX       float64
	PX       int64
}

func NewLock(dmap, key string, deadline float64) *Lock {
	return &Lock{
		DMap:     dmap,
		Key:      key,
		Deadline: deadline,
	}
}

func (l *Lock) SetEX(ex float64) *Lock {
	l.EX = ex
	return l
}

func (l *Lock) SetPX(px int64) *Lock {
	l.PX = px
	return l
}

func (l *Lock) Command(ctx context.Context) *redis.StringCmd {
	var args []interface{}
	args = append(args, DMap.Lock)
	args = append(args, l.DMap)
	args = append(args, l.Key)
	args = append(args, l.Deadline)

	// Options
	if l.EX != 0 {
		args = append(args, "EX")
		args = append(args, l.EX)
	}

	if l.PX != 0 {
		args = append(args, "PX")
		args = append(args, l.PX)
	}

	return redis.NewStringCmd(ctx, args...)
}

type Unlock struct {
	DMap  string
	Key   string
	Token string
}

func NewUnlock(dmap, key, token string) *Unlock {
	return &Unlock{
		DMap:  dmap,
		Key:   key,
		Token: token,
	}
}

func (u *Unlock) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.Unlock)
	args = append(args, u.DMap)
	args = append(args, u.Key)
	args = append(args, u.Token)
	return redis.NewStatusCmd(ctx, args...)
}

// TODO: Add PLockLease

type LockLease struct {
	DMap    string
	Key     string
	Token   string
	Timeout float64
}

func NewLockLease(dmap, key, token string, timeout float64) *LockLease {
	return &LockLease{
		DMap:    dmap,
		Key:     key,
		Token:   token,
		Timeout: timeout,
	}
}

func (l *LockLease) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.LockLease)
	args = append(args, l.DMap)
	args = append(args, l.Key)
	args = append(args, l.Token)
	args = append(args, l.Timeout)
	return redis.NewStatusCmd(ctx, args...)
}

type PLockLease struct {
	DMap    string
	Key     string
	Token   string
	Timeout int64
}

func NewPLockLease(dmap, key, token string, timeout int64) *PLockLease {
	return &PLockLease{
		DMap:    dmap,
		Key:     key,
		Token:   token,
		Timeout: timeout,
	}
}

func (p *PLockLease) Command(ctx context.Context) *redis.StatusCmd {
	var args []interface{}
	args = append(args, DMap.PLockLease)
	args = append(args, p.DMap)
	args = append(args, p.Key)
	args = append(args, p.Token)
	args = append(args, p.Timeout)
	return redis.NewStatusCmd(ctx, args...)
}
