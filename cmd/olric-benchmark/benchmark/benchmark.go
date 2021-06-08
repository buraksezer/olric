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
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/config"
	_serializer "github.com/buraksezer/olric/serializer"
)

type Benchmark struct {
	mu          sync.RWMutex
	responses   []time.Duration
	requests    int
	connections int
	serializer  string
	client      *client.Client
	log         *log.Logger
	wg          sync.WaitGroup
}

func New(address,
	timeout,
	serializer string,
	conns, requests int,
	logger *log.Logger) (*Benchmark, error) {
	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var s _serializer.Serializer
	switch {
	case serializer == "json":
		s = _serializer.NewJSONSerializer()
	case serializer == "msgpack":
		s = _serializer.NewMsgpackSerializer()
	case serializer == "gob":
		s = _serializer.NewGobSerializer()
	default:
		return nil, fmt.Errorf("invalid serializer: %s", serializer)
	}

	dt, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}
	cc := &client.Config{
		Servers:    strings.Split(address, ","),
		Serializer: s,
		Client: &config.Client{
			DialTimeout: dt,
			MaxConn:     conns,
		},
	}

	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}

	return &Benchmark{
		responses:   []time.Duration{},
		requests:    requests,
		connections: conns,
		client:      c,
		serializer:  serializer,
		log:         logger,
	}, nil
}

func (b *Benchmark) stats(cmd string, elapsed time.Duration) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.log.Printf("### STATS FOR COMMAND: %s ###", strings.ToUpper(cmd))
	b.log.Printf("Serializer is %s", b.serializer)
	b.log.Printf("%d requests completed in %v", b.requests, elapsed)
	b.log.Printf("%d parallel clients", b.connections)
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
	rps := float64(b.requests) / (float64(elapsed) / float64(time.Second))
	b.log.Printf("\n%f requests per second\n", rps)
}

func (b *Benchmark) worker(cmd string, ch chan int) {
	defer b.wg.Done()

	dm := b.client.NewDMap("olric-benchmark-test")
	for i := range ch {
		now := time.Now()
		switch {
		case strings.ToLower(cmd) == "put":
			if err := dm.Put(strconv.Itoa(i), i); err != nil {
				b.log.Printf("[ERROR] Failed to call Put command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "get":
			_, err := dm.Get(strconv.Itoa(i))
			if err != nil {
				b.log.Printf("[ERROR] Failed to call Get command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "delete":
			err := dm.Delete(strconv.Itoa(i))
			if err != nil {
				b.log.Printf("[ERROR] Failed to call Delete command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "incr":
			_, err := dm.Incr(strconv.Itoa(i), 1)
			if err != nil {
				b.log.Printf("[ERROR] Failed to call Incr command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "decr":
			_, err := dm.Decr(strconv.Itoa(i), 1)
			if err != nil {
				b.log.Printf("[ERROR] Failed to call Decr command for %d: %v", i, err)
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
	for _, c := range []string{"put", "get", "delete"} {
		if strings.EqualFold(c, cmd) {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("invalid command: %s", cmd)
	}

	var elapsed time.Duration
	ch := make(chan int)
	for i := 0; i < b.connections; i++ {
		b.wg.Add(1)
		go b.worker(cmd, ch)
	}

	now := time.Now()
	for i := 0; i < b.requests; i++ {
		ch <- i
	}
	close(ch)
	b.wg.Wait()

	elapsed = time.Since(now)
	b.stats(cmd, elapsed)
	return nil
}
