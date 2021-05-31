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
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/pkg/storage"
)
// TODO: Add compaction

// https://redislabs.com/ebook/part-2-core-concepts/chapter-4-keeping-data-safe-and-ensuring-performance/4-1-persistence-options/4-1-3-rewritingcompacting-append-only-files/

var ErrClosed = errors.New("journal is closed")

const chanSize = 2 << 15

type OpCode uint8

const (
	OpPut = OpCode(iota) + 1
	OpUpdateTTL
	OpDelete
)

type Entry struct {
	OpCode OpCode
	HKey   uint64
	Value  storage.Entry
}

const HeaderLen = 13

type Header struct {
	HKey     uint64 // 8 bytes
	ValueLen uint32 // 4 bytes
	OpCode   OpCode // 1 byte
}

type Config struct {
	Path        string
	FSyncPolicy int
	ReplayDone  func()
}

type Stats struct {
	Put       uint64
	UpdateTTL uint64
	Delete    uint64
	QueueLen  int64
}

type stats struct {
	put       uint64
	updateTTL uint64
	delete    uint64
	queueLen  int64
}

type Journal struct {
	config *Config
	file   *os.File
	queue  chan *Entry
	stats  *stats
	pool   *bufpool.BufPool
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func New(c *Config) (*Journal, error) {
	f, err := os.OpenFile(c.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	if c.FSyncPolicy == 0 {
		c.FSyncPolicy = DefaultFsyncPolicy
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Journal{
		config: c,
		file:   f,
		queue:  make(chan *Entry, chanSize),
		stats:  &stats{},
		pool:   bufpool.New(),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (j *Journal) isAlive() bool {
	select {
	case <-j.ctx.Done():
		return false
	default:
		return true
	}
}

func (j *Journal) Start() error {
	if !j.isAlive() {
		return ErrClosed
	}

	if j.config.FSyncPolicy == FsyncEverySecond {
		j.wg.Add(1)
		go j.fsyncPeriodically(time.Second)
	}

	// Start the consumer
	j.wg.Add(1)
	go j.consumer(j.queue)

	return nil
}

func (j *Journal) Append(opcode OpCode, hkey uint64, value storage.Entry) error {
	e := &Entry{
		OpCode: opcode,
		HKey:   hkey,
		Value:  value,
	}
	if !j.isAlive() {
		return ErrClosed
	}
	j.queue <- e
	atomic.AddInt64(&j.stats.queueLen, 1)
	return nil
}

func (j *Journal) statsAfterProcessed(opcode OpCode) {
	switch opcode {
	case OpPut:
		atomic.AddUint64(&j.stats.put, 1)
	case OpDelete:
		atomic.AddUint64(&j.stats.delete, 1)
	case OpUpdateTTL:
		atomic.AddUint64(&j.stats.updateTTL, 1)
	}
	atomic.AddInt64(&j.stats.queueLen, -1)
}

func (j *Journal) append(opcode OpCode, hkey uint64, value storage.Entry) error {
	val := value.Encode()
	h := Header{
		OpCode:   opcode,
		HKey:     hkey,
		ValueLen: uint32(len(val)),
	}

	buf := j.pool.Get()
	defer j.pool.Put(buf)

	err := binary.Write(buf, binary.BigEndian, h)
	if err != nil {
		return err
	}

	_, err = buf.Write(val)
	if err != nil {
		return err
	}

	_, err = j.file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	if j.config.FSyncPolicy == FsyncAlways {
		j.fsync()
	}

	return nil
}

func (j *Journal) processEntry(e *Entry) {
	defer j.statsAfterProcessed(e.OpCode)

	err := j.append(e.OpCode, e.HKey, e.Value)
	if err != nil {
		// TODO: Log this event properly
		fmt.Println("append returned an error:", err)
	}
}

func (j *Journal) drainQueue(ch chan *Entry) {
	ctx, cancel := context.WithCancel(context.Background())
	j.wg.Add(1)
	go func() {
		defer j.wg.Done()
		defer cancel()

		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()

		for {
			timer.Reset(10 * time.Millisecond)
			if <-timer.C; true {
				if atomic.LoadInt64(&j.stats.queueLen) == 0 {
					return
				}
			}
		}
	}()

	for {
		select {
		case e := <-ch:
			j.processEntry(e)
		case <-ctx.Done():
			return
		}
	}
}

func (j *Journal) consumer(entries chan *Entry) {
	defer j.wg.Done()

	for {
		select {
		case e := <-entries:
			j.processEntry(e)
		case <-j.ctx.Done():
			// journal is closed
			j.drainQueue(entries)
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
	return j.file.Close()
}

func (j *Journal) Destroy() error {
	err := j.Close()
	if err != nil {
		return err
	}

	return os.Remove(j.file.Name())
}
