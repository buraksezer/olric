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

import (
	"log"
)

type TransferIterator interface {
	Next() bool

	Export() ([]byte, error)

	Pop() error
}

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

	// PutRaw inserts an encoded entry into the storage engine.
	PutRaw(uint64, []byte) error

	// Put inserts a new Entry into the storage engine.
	Put(uint64, Entry) error

	// GetRaw reads an encoded entry from the storage engine.
	GetRaw(uint64) ([]byte, error)

	// Get reads an entry from the storage engine.
	Get(uint64) (Entry, error)

	// GetTTL extracts TTL of an entry.
	GetTTL(uint64) (int64, error)

	// GetLastAccess extracts LastAccess of an entry.
	GetLastAccess(uint64) (int64, error)

	// GetKey extracts key of an entry.
	GetKey(uint64) (string, error)

	// Delete deletes an entry from the storage engine.
	Delete(uint64) error

	// UpdateTTL updates TTL of an entry. It returns ErrKeyNotFound,
	// if the key doesn't exist.
	UpdateTTL(uint64, Entry) error

	TransferIterator() TransferIterator

	Import([]byte, func(uint64, Entry) error) error

	// Stats returns metrics for an online storage engine.
	Stats() Stats

	// Check returns true, if the key exists.
	Check(uint64) bool

	// Range implements a loop over the storage engine
	Range(func(uint64, Entry) bool)

	// RegexMatchOnKeys runs a regular expression over keys and loops over the result.
	RegexMatchOnKeys(string, func(uint64, Entry) bool) error

	// Compaction reorganizes storage tables and reclaims wasted resources.
	Compaction() (bool, error)

	// Close stops an online storage engine instance. It may free some of allocated
	// resources. A storage engine implementation should be started again, but it
	// depends on the implementation.
	Close() error

	// Destroy stops an online storage engine instance and frees allocated resources.
	// It should not be possible to reuse a destroyed storage engine.
	Destroy() error
}
