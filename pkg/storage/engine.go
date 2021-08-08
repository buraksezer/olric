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

package storage

import "log"

// Engine defines methods for a storage engine implementation.
type Engine interface {
	// SetConfig sets a storage engine configuration. nil can be accepted, but
	// it depends on the implementation.
	SetConfig(*Config)

	// SetLogger sets a logger. nil can be accepted, but it depends on the implementation.
	SetLogger(*log.Logger)

	// Start can be used to run background services before starting operation.
	Start() error

	// NewEntry returns a new Entry interface implemented by the current storage
	// engine implementation.
	NewEntry() Entry

	// Name returns name of the current storage engine implementation.
	Name() string

	// Fork creates an empty instance of an online engine by using the current
	// configuration.
	Fork(*Config) (Engine, error)

	PutRaw(uint64, []byte) error

	// Put inserts a new Entry into the storage engine.
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
	Compaction() (bool, error)
	Close() error
	Destroy() error
}
