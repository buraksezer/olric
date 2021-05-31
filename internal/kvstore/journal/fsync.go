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

import "time"

const (
	FsyncAlways = iota + 1
	FsyncEverySecond
	FsyncDisabled
)

const DefaultFsyncPolicy = FsyncEverySecond

func (j *Journal) fsync() {
	err := j.file.Sync()
	if err != nil {
		// TODO: log this event
	}
}

func (j *Journal) fsyncPeriodically(d time.Duration) {
	defer j.wg.Done()
	timer := time.NewTimer(d)
	defer timer.Stop()

	for {
		timer.Reset(d)
		select {
		case <-timer.C:
			j.fsync()
		case <-j.ctx.Done():
			// flush the data last time.
			j.fsync()
			return
		}
	}
}
