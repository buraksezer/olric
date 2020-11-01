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

package engine

import "errors"

// ErrFragmented is an error that indicates this storage instance is currently
// fragmented and it cannot be serialized.
var ErrFragmented = errors.New("storage fragmented")

// ErrKeyTooLarge is an error that indicates the given key is large than the determined key size.
// The current maximum key length is 256.
var ErrKeyTooLarge = errors.New("key too large")

// ErrKeyNotFound is an error that indicates that the requested key could not be found in the DB.
var ErrKeyNotFound = errors.New("key not found")

type Options map[string]interface{}

type Entry interface {
	SetKey(string)
	Key() string
	SetValue([]byte)
	Value() []byte
	SetTTL(int64)
	TTL() int64
	SetTimestamp(int642 int64)
	Timestamp() int64
	Encode() []byte
	Decode([]byte)
}

type Engine interface {
	GetInstance() Engine
	Name() string
	NewEntry() Entry
	PutRaw(uint64, []byte) error
	Put(uint64, Entry) error
	GetRaw(uint64) ([]byte, error)
	Get(uint64) (Entry, error)
	GetTTL(uint64) (int64, error)
	GetKey(uint64) (string, error)
	Delete(uint64) error
	UpdateTTL(uint64, Entry) error
	Export() ([]byte, error)
	Len() int
	Stats() map[string]int
	NumTables() int
	Inuse() int
	Check(uint64) bool
	Range(func(uint64, Entry) bool)
	MatchOnKey(string, func(uint64, Entry) bool) error
	CompactTables() bool
	Close() error
}
