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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/client"
	"github.com/buraksezer/olric/config"
	_serializer "github.com/buraksezer/olric/serializer"
)

type Loader struct {
	mu         sync.RWMutex
	responses  []int
	commands   []string
	keyCount   int
	numClients int
	serializer string
	client     *client.Client
	log        *log.Logger
	wg         sync.WaitGroup
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
		responses:  []int{},
		keyCount:   keyCount,
		numClients: numClients,
		client:     c,
		serializer: serializer,
		log:        logger,
	}
	return l, nil
}

func (l *Loader) stats(cmd string, elapsed time.Duration) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	l.log.Printf("### STATS FOR COMMAND: %s ###", strings.ToUpper(cmd))
	l.log.Printf("Serializer is %s", l.serializer)
	l.log.Printf("%d requests completed in %v", l.keyCount, elapsed)
	l.log.Printf("%d parallel clients", l.numClients)
	l.log.Printf("\n")

	var ms int = 1000000
	result := make(map[int]int)
	for _, t := range l.responses {
		result[(t/ms)+1]++
	}
	var keys []int
	for key, _ := range result {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	for _, key := range keys {
		count := result[key]
		percentage := count * 100 / len(l.responses)
		if percentage > 0 {
			fmt.Printf("%4d%%%2s<=%3d milliseconds\n", percentage, "", key)
		}
	}
}

func (l *Loader) call(cmd string, ch chan int) {
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
		l.responses = append(l.responses, int(response))
		l.mu.Unlock()
	}
}

func (l *Loader) Run(cmd string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("no command given")
	}
	var found bool
	for _, c := range []string{"put", "get", "delete"} {
		if strings.ToUpper(c) == strings.ToUpper(cmd) {
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
		go l.call(cmd, ch)
	}

	now := time.Now()
	for i := 0; i < l.keyCount; i++ {
		ch <- i
	}
	close(ch)
	l.wg.Wait()

	elapsed = time.Since(now)
	l.stats(cmd, elapsed)
	return nil
}
