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

package loader

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

type Loader struct {
	mu          sync.RWMutex
	responses   []time.Duration
	commands    []string
	numRequests int
	numClients  int
	serializer  string
	client      *client.Client
	log         *log.Logger
	wg          sync.WaitGroup
}

func New(addrs, timeout, serializer string,
	numClients, keyCount int, logger *log.Logger) (*Loader, error) {
	// Default serializer is Gob serializer, just set nil or use gob keyword to use it.
	var s _serializer.Serializer
	if serializer == "json" {
		s = _serializer.NewJSONSerializer()
	} else if serializer == "msgpack" {
		s = _serializer.NewMsgpackSerializer()
	} else if serializer == "gob" {
		s = _serializer.NewGobSerializer()
	} else {
		return nil, fmt.Errorf("invalid serializer: %s", serializer)
	}

	dt, err := time.ParseDuration(timeout)
	if err != nil {
		return nil, err
	}
	cc := &client.Config{
		Servers:    strings.Split(addrs, ","),
		Serializer: s,
		Client: &config.Client{
			DialTimeout: dt,
			MaxConn:     numClients,
		},
	}
	c, err := client.New(cc)
	if err != nil {
		return nil, err
	}
	l := &Loader{
		responses:   []time.Duration{},
		numRequests: keyCount,
		numClients:  numClients,
		client:      c,
		serializer:  serializer,
		log:         logger,
	}
	return l, nil
}

func (l *Loader) stats(cmd string, elapsed time.Duration) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	l.log.Printf("### STATS FOR COMMAND: %s ###", strings.ToUpper(cmd))
	l.log.Printf("Serializer is %s", l.serializer)
	l.log.Printf("%d requests completed in %v", l.numRequests, elapsed)
	l.log.Printf("%d parallel clients", l.numClients)
	l.log.Printf("\n")

	var limit time.Duration
	var lastper float64
	for {
		limit += time.Millisecond
		var hits, count int
		for _, rtime := range l.responses {
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
	rps := float64(l.numRequests) / (float64(elapsed) / float64(time.Second))
	l.log.Printf("\n%f requests per second\n", rps)
}

func (l *Loader) worker(cmd string, ch chan int) {
	defer l.wg.Done()

	dm := l.client.NewDMap("olric-load-test")
	for i := range ch {
		now := time.Now()
		switch {
		case strings.ToLower(cmd) == "put":
			if err := dm.Put(strconv.Itoa(i), i); err != nil {
				l.log.Printf("[ERROR] Failed to call Put command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "get":
			_, err := dm.Get(strconv.Itoa(i))
			if err != nil {
				l.log.Printf("[ERROR] Failed to call Get command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "delete":
			err := dm.Delete(strconv.Itoa(i))
			if err != nil {
				l.log.Printf("[ERROR] Failed to call Delete command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "incr":
			_, err := dm.Incr(strconv.Itoa(i), 1)
			if err != nil {
				l.log.Printf("[ERROR] Failed to call Incr command for %d: %v", i, err)
			}
		case strings.ToLower(cmd) == "decr":
			_, err := dm.Decr(strconv.Itoa(i), 1)
			if err != nil {
				l.log.Printf("[ERROR] Failed to call Decr command for %d: %v", i, err)
			}
		}

		response := time.Since(now)
		l.mu.Lock()
		l.responses = append(l.responses, response)
		l.mu.Unlock()
	}
}

func (l *Loader) Run(cmd string) error {
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
	for i := 0; i < l.numClients; i++ {
		l.wg.Add(1)
		go l.worker(cmd, ch)
	}

	now := time.Now()
	for i := 0; i < l.numRequests; i++ {
		ch <- i
	}
	close(ch)
	l.wg.Wait()

	elapsed = time.Since(now)
	l.stats(cmd, elapsed)
	return nil
}
