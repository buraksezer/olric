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

package dmap

import (
	"strings"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
)

func (s *Service) callCompactionOnFragment(f *fragment) bool {
	for {
		f.Lock()
		done, err := f.Compaction()
		if err != nil {
			f.Unlock()
			// Continue
			return true
		}
		f.Unlock()

		if done {
			return true
		}

		select {
		case <-s.ctx.Done():
			// Break
			return false
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func (s *Service) doCompaction(ch chan uint64, wg *sync.WaitGroup) {
	defer wg.Done()

	compaction := func(part *partitions.Partition) {
		part.Map().Range(func(name, tmp interface{}) bool {
			if !strings.HasPrefix(name.(string), "dmap.") {
				// Continue. This fragment belongs to a different data structure.
				return true
			}

			f := tmp.(*fragment)
			return s.callCompactionOnFragment(f)
		})
	}

	for {
		select {
		case <-s.ctx.Done():
			return
		case partID := <-ch:
			part := s.primary.PartitionByID(partID)
			compaction(part)

			backup := s.backup.PartitionByID(partID)
			compaction(backup)
		}
	}
}

func (s *Service) triggerCompaction() {
	var wg sync.WaitGroup

	ch := make(chan uint64, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go s.doCompaction(ch, &wg)
	}

	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		select {
		case <-s.ctx.Done():
			break
		case ch <- partID:
		}
	}

	wg.Wait()
}

func (s *Service) compactionWorker() {
	defer s.wg.Done()

	timer := time.NewTimer(s.config.DMaps.TriggerCompactionInterval)
	defer timer.Stop()

	for {
		timer.Reset(s.config.DMaps.TriggerCompactionInterval)
		select {
		case <-timer.C:
			s.triggerCompaction()
		case <-s.ctx.Done():
			return
		}
	}
}
