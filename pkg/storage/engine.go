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

package storage

type Engine interface {
	SetConfig(*Config)
	Start() error
	NewEntry() Entry
	Name() string
	Fork(*Config) (Engine, error)
	PutRaw(uint64, []byte) error
	Put(uint64, Entry) error
	GetRaw(uint64) ([]byte, error)
	Get(uint64) (Entry, error)
	GetTTL(uint64) (int64, error)
	GetKey(uint64) (string, error)
	Delete(uint64) error
	UpdateTTL(uint64, Entry) error
	Import([]byte) (Engine, error)
	Export() ([]byte, error)
	Stats() Stats
	Check(uint64) bool
	Range(func(uint64, Entry) bool)
	RegexMatchOnKeys(string, func(uint64, Entry) bool) error
	Compaction() bool
	Close() error
}
