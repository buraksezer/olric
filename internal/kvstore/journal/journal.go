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

package journal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/pkg/storage"
)

const chanSize = 2 << 13

type OpCode uint8

const (
	OpPUT = OpCode(iota) + 1
	OpUPDATETTL
	OpDELETE
)

const HeaderLen = 4

type Entry struct {
	OpCode OpCode
	HKey   uint64
	Value  storage.Entry
}

type Header struct {
	OpCode   OpCode
	HKey     uint64
	ValueLen uint32
}

type Config struct {
	Path string
}

type Stats struct {
	Put       uint64
	UpdateTTL uint64
	Delete    uint64
}

type stats struct {
	put       uint64
	updateTTL uint64
	delete    uint64
}

type chans struct {
	put       chan *Entry
	updateTTL chan *Entry
	delete    chan *Entry
}

type Journal struct {
	config  *Config
	file    *os.File
	chans   *chans
	stats   *stats
	bufpool *bufpool.BufPool
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func New(c *Config) (*Journal, error) {
	f, err := os.OpenFile(c.Path, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Journal{
		config: c,
		file:   f,
		chans: &chans{
			put:       make(chan *Entry, chanSize),
			updateTTL: make(chan *Entry, chanSize),
			delete:    make(chan *Entry, chanSize),
		},
		stats:   &stats{},
		bufpool: bufpool.New(),
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (j *Journal) Start() error {
	select {
	case <-j.ctx.Done():
		return errors.New("journal is closed")
	default:
	}
	// Start background workers
	j.wg.Add(1)
	go j.worker(j.chans.put)

	j.wg.Add(1)
	go j.worker(j.chans.delete)

	j.wg.Add(1)
	go j.worker(j.chans.updateTTL)
	return nil
}

func (j *Journal) Append(opcode OpCode, hkey uint64, value storage.Entry) {
	e := &Entry{
		OpCode: opcode,
		HKey:   hkey,
		Value:  value,
	}
	switch opcode {
	case OpPUT:
		j.chans.put <- e
	case OpDELETE:
		j.chans.delete <- e
	case OpUPDATETTL:
		j.chans.updateTTL <- e
	}
}

func (j *Journal) updateStats(opcode OpCode) {
	switch opcode {
	case OpPUT:
		atomic.AddUint64(&j.stats.put, 1)
	case OpDELETE:
		atomic.AddUint64(&j.stats.delete, 1)
	case OpUPDATETTL:
		atomic.AddUint64(&j.stats.updateTTL, 1)
	}
}

func (j *Journal) append(opcode OpCode, hkey uint64, value storage.Entry) error {
	val := value.Encode()
	h := Header{
		OpCode:   opcode,
		HKey:     hkey,
		ValueLen: uint32(HeaderLen + len(val)),
	}
	buf := j.bufpool.Get()
	defer j.bufpool.Put(buf)

	err := binary.Write(buf, binary.BigEndian, h)
	if err != nil {
		return err
	}
	_, err = buf.Write(val)
	if err != nil {
		return err
	}
	_, err = j.file.Write(buf.Bytes())
	return err
}

func (j *Journal) worker(ch chan *Entry) {
	defer j.wg.Done()
	for {
		select {
		case e := <-ch:
			err := j.append(e.OpCode, e.HKey, e.Value)
			if err != nil {
				// TODO: Log this event properly
				fmt.Println("append returned an error:", err)
			}
			j.updateStats(e.OpCode)
		case <-j.ctx.Done():
			// journal is closed
			return
		}
	}
}

func (j *Journal) Stats() Stats {
	return Stats{
		Put:       atomic.LoadUint64(&j.stats.put),
		Delete:    atomic.LoadUint64(&j.stats.delete),
		UpdateTTL: atomic.LoadUint64(&j.stats.updateTTL),
	}
}

func (j *Journal) Close() error {
	select {
	case <-j.ctx.Done():
		return nil
	default:
	}
	j.cancel()
	j.wg.Wait()
	err := j.file.Close()
	return err
}
