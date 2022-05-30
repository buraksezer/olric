// Copyright 2018-2022 Burak Sezer
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

package resolver

import "github.com/cespare/xxhash/v2"

type Kind int

const (
	ReadCommandKind = Kind(iota + 1)
	MutateCommandKind
)

type Tx interface {
	Start()
	Stop()
	Commit(readVersion, commitVersion int64, commands []*Key) error
}

type Key struct {
	HKey uint64
	Key  string
	Kind Kind
}

func NewKey(key string, kind Kind) *Key {
	return &Key{
		HKey: xxhash.Sum64String(key),
		Key:  key,
		Kind: kind,
	}
}
