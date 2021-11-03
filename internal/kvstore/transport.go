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

package kvstore

import (
	"fmt"
	"io"

	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

type transferIterator struct {
	kvstore *KVStore
}

func (t *transferIterator) Next() bool {
	return len(t.kvstore.tables) != 0
}

func (t *transferIterator) Pop() error {
	if len(t.kvstore.tables) == 0 {
		return fmt.Errorf("there is no table to pop")
	}

	t.kvstore.tables = append(t.kvstore.tables[:0], t.kvstore.tables[1:]...)

	return nil
}

func (t *transferIterator) Export() ([]byte, error) {
	for _, t := range t.kvstore.tables {
		if t.State() == table.RecycledState {
			continue
		}

		return table.Encode(t)
	}
	return nil, io.EOF
}

func (t *transferIterator) Merge(data []byte, f func(uint64, storage.Entry) error) error {
	tb, err := table.Decode(data)
	if err != nil {
		return err
	}

	if t.kvstore.Stats().Length == 0 {
		// DMap has no keys. Set the imported storage instance.
		// The old one will be garbage collected.
		t.kvstore.AppendTable(tb)
		return nil
	}

	t.kvstore.Range(func(hkey uint64, entry storage.Entry) bool {
		err = f(hkey, entry)
		return err == nil
	})

	return err
}

func (k *KVStore) TransferIterator() storage.TransferIterator {
	return &transferIterator{
		kvstore: k,
	}
}
