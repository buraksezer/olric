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

package zmap

import "github.com/buraksezer/olric/internal/resolver"

// 1- Get read version from sequencer
// 2- Receive commands: Put, Get, etc...
// 3- Commit
// 4- Get commit version from sequencer
// 5- Send all these things to the resolver
// 6- Send mutations to the transaction-log server
// 7- Pull changes from the transaction log server

func (tx *Transaction) Put(key, value []byte) {
	tx.mtx.Lock()
	defer tx.mtx.Unlock()

	c := &command{
		kind:  resolver.MutateCommandKind,
		key:   key,
		value: value,
	}
	tx.commands = append(tx.commands, c)
}
