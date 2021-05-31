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

package flakeid

import (
	"encoding/binary"
	"encoding/hex"
	"sync"
	"time"
)

type FlakeID struct {
	mu sync.Mutex

	mac string
	latest int64
	seq uint16
}

func New(mac string) *FlakeID {
	return &FlakeID{mac: mac}
}

func (f *FlakeID) NewID() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	ms := time.Now().UnixNano() / 1000000
	if ms > f.latest {
		f.seq = 0
		f.latest = ms
	} else {
		f.seq++
	}

	src := make([]byte, 16)
	binary.BigEndian.PutUint64(src[:8], uint64(f.latest))

	copy(src[8:14], f.mac)

	binary.BigEndian.PutUint16(src[14:], f.seq)

	return hex.EncodeToString(src)
}
