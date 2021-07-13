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

package benchmark

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/config"
	"github.com/buraksezer/olric/serializer"
)

var ErrBenchmarkInterrupted = errors.New("benchmark interrupted")

type Args struct {
	Address     string
	Requests    int
	Connections int
	Timeout     string
	Test        string
	Serializer  string
	DataSize    int
	KeepGoing   bool
}

type Benchmark struct {
	mu sync.RWMutex

	responses []time.Duration
	args      *Args
	client    *client.Client
	log       *log.Logger
	wg        sync.WaitGroup
}

func New(args *Args, logger *log.Logger) (*Benchmark, error) {
	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var s serializer.Serializer
	switch args.Serializer {
	case "json":
		s = serializer.NewJSONSerializer()
	case "msgpack":
		s = serializer.NewMsgpackSerializer()
	case "gob":
		s = serializer.NewGobSerializer()
	default:
		return nil, fmt.Errorf("invalid serializer: %s", args.Serializer)
	}

	dt, err := time.ParseDuration(args.Timeout)
	if err != nil {
		return nil, err
	}
	cc := &client.Config{
		Servers:    strings.Split(args.Address, ","),
		Serializer: s,
		Client: &config.Client{
			DialTimeout: dt,
			MaxConn:     args.Connections,
		},
	}

	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}

	return &Benchmark{
		responses: []time.Duration{},
		client:    c,
		args:      args,
		log:       logger,
	}, nil
}

func (b *Benchmark) stats(cmd string, elapsed time.Duration) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.log.Printf("### STATS FOR COMMAND: %s ###", strings.ToUpper(cmd))
	b.log.Printf("Serializer is %s", b.args.Serializer)
	b.log.Printf("%d requests completed in %v", b.args.Requests, elapsed)
	b.log.Printf("%d parallel clients", b.args.Connections)
	b.log.Printf("\n")

	var limit time.Duration
	var lastper float64
	for {
		limit += time.Millisecond
		var hits, count int
		for _, rtime := range b.responses {
			if rtime < limit {
				hits++
			}
			count++
		}
		per := float64(hits) / float64(count)
		if math.Floor(per*10000) == math.Floor(lastper*10000) {
			continue
		}
		lastper = per
		fmt.Printf("%.2f%% <= %d milliseconds\n", per*100, (limit-time.Millisecond)/time.Millisecond)
		if per == 1.0 {
			break
		}
	}
	rps := float64(b.args.Requests) / (float64(elapsed) / float64(time.Second))
	b.log.Printf("\n%f requests per second\n", rps)
}

func (b *Benchmark) worker(cancel context.CancelFunc, cmd string, ch chan int) {
	defer b.wg.Done()

	var err error
	dm := b.client.NewDMap("olric-benchmark-test")
	value := make([]byte, b.args.DataSize)
	for i := range ch {
		key := fmt.Sprintf("%06d", i)
		now := time.Now()
		switch {
		case strings.ToLower(cmd) == "put":
			err = dm.Put(key, value)
		case strings.ToLower(cmd) == "get":
			_, err = dm.Get(key)
		case strings.ToLower(cmd) == "delete":
			err = dm.Delete(key)
		case strings.ToLower(cmd) == "incr":
			_, err = dm.Incr(key, 1)
		case strings.ToLower(cmd) == "decr":
			_, err = dm.Decr(key, 1)
		}
		if err != nil {
			b.log.Printf("[ERROR] olric-benchmark: %s: %s: %v", cmd, key, err)
			if !b.args.KeepGoing {
				cancel()
				return
			}
		}

		response := time.Since(now)
		b.mu.Lock()
		b.responses = append(b.responses, response)
		b.mu.Unlock()
	}
}

func (b *Benchmark) Run(cmd string) error {
	if cmd == "" {
		return fmt.Errorf("no command given")
	}
	var found bool
	for _, c := range []string{"put", "get", "delete", "incr", "decr"} {
		if strings.EqualFold(c, cmd) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("invalid command: %s", cmd)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var elapsed time.Duration
	ch := make(chan int)
	for i := 0; i < b.args.Connections; i++ {
		b.wg.Add(1)
		go b.worker(cancel, cmd, ch)
	}

	now := time.Now()
	for i := 0; i < b.args.Requests; i++ {
		select {
		case <-ctx.Done():
			return ErrBenchmarkInterrupted
		case ch <- i:
		}
	}
	close(ch)
	b.wg.Wait()

	elapsed = time.Since(now)
	b.stats(cmd, elapsed)
	return nil
}
