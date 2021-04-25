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
	"time"
)

type accessLog struct {
	m map[uint64]int64
}

func newAccessLog() *accessLog {
	return &accessLog{
		m: make(map[uint64]int64),
	}
}

func (a *accessLog) touch(hkey uint64) {
	a.m[hkey] = time.Now().UnixNano()
}

func (a *accessLog) get(hkey uint64) (int64, bool) {
	timestamp, ok := a.m[hkey]
	return timestamp, ok
}

func (a *accessLog) delete(hkey uint64) {
	delete(a.m, hkey)
}

func (a *accessLog) iterator(f func(hkey uint64, timestamp int64) bool) {
	for hkey, timestamp := range a.m {
		if !f(hkey, timestamp) {
			break
		}
	}
}
