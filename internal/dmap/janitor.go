// Copyright 2018-2020 Burak Sezer
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
	"time"

	"github.com/buraksezer/olric/internal/cluster/partitions"
)

func (s *Service) deleteEmptyFragments() {
	janitor := func(part *partitions.Partition) {
		part.Map().Range(func(name, tmp interface{}) bool {
			f := tmp.(*fragment)
			f.Lock()
			defer f.Unlock()
			if f.storage.Stats().Length != 0 {
				// Continue scanning.
				return true
			}
			part.Map().Delete(name)
			s.log.V(4).Printf("[INFO] Empty DMap fragment (kind: %s) has been deleted: %s on PartID: %d",
				part.Kind(), name, part.Id())
			return true
		})
	}
	for partID := uint64(0); partID < s.config.PartitionCount; partID++ {
		// Clean stale dmaps on partition table
		part := s.primary.PartitionById(partID)
		janitor(part)
		// Clean stale dmaps on backup partition table
		backup := s.backup.PartitionById(partID)
		janitor(backup)
	}
}

func (s *Service) janitor() {
	defer s.wg.Done()
	// TODO: interval should be parametric.
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	for {
		timer.Reset(50 * time.Millisecond)
		select {
		case <-timer.C:
			s.deleteEmptyFragments()
		case <-s.ctx.Done():
			return
		}
	}
}
