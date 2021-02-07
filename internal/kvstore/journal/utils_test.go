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
	"encoding/json"
	"github.com/buraksezer/olric/pkg/storage"
)

// MockEntry represents a Value_ with its metadata.
type MockEntry struct {
	Key_       string `json:"key"`
	TTL_       int64  `json:"ttl"`
	Timestamp_ int64  `json:"timestamp"`
	Value_     []byte `json:"value"`
}

var _ storage.Entry = (*MockEntry)(nil)

func NewMockEntry() *MockEntry {
	return &MockEntry{}
}

func (e *MockEntry) SetKey(key string) {
	e.Key_ = key
}

func (e *MockEntry) Key() string {
	return e.Key_
}

func (e *MockEntry) SetValue(value []byte) {
	e.Value_ = value
}

func (e *MockEntry) Value() []byte {
	return e.Value_
}

func (e *MockEntry) SetTTL(ttl int64) {
	e.TTL_ = ttl
}

func (e *MockEntry) TTL() int64 {
	return e.TTL_
}

func (e *MockEntry) SetTimestamp(timestamp int64) {
	e.Timestamp_ = timestamp
}

func (e *MockEntry) Timestamp() int64 {
	return e.Timestamp_
}

func (e *MockEntry) Encode() []byte {
	data, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return data
}

func (e *MockEntry) Decode(buf []byte) {
	err := json.Unmarshal(buf, e)
	if err != nil {
		panic(err)
	}
}
