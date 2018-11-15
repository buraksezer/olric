// Copyright 2018 Burak Sezer
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

package snapshot

import (
	"encoding/binary"
	"errors"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

var (
	//ErrLoaderDone indicates that no more value left in the snapshot to restore.
	ErrLoaderDone = errors.New("loader done")

	// ErrFirstRun indicates that there is no key in the snapshot to restore. Run for the first time.
	ErrFirstRun = errors.New("first run")
)

// DMap represents a DMap object which's restored from snapshot.
type DMap struct {
	PartID uint64
	Name   string
	Off    *offheap.Offheap
}

// Loader implements an iterator like mechanism to restore dmaps from snapshot.
type Loader struct {
	s     *Snapshot
	dmaps map[uint64]map[string]struct{}
}

// NewLoader creates and returns a new Loader.
func (s *Snapshot) NewLoader(dkey []byte) (*Loader, error) {
	var dmaps map[uint64]map[string]struct{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dkey)
		if err == badger.ErrKeyNotFound {
			return ErrFirstRun
		}
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(value, &dmaps)
	})
	if err != nil {
		return nil, err
	}
	return &Loader{
		s:     s,
		dmaps: dmaps,
	}, nil
}

func (l *Loader) loadFromBadger(hkeys map[uint64]struct{}) (*offheap.Offheap, error) {
	o, err := offheap.New(0)
	if err != nil {
		return nil, err
	}
	err = l.s.db.View(func(txn *badger.Txn) error {
		bkey := make([]byte, 8)
		for hkey := range hkeys {
			binary.BigEndian.PutUint64(bkey, hkey)
			item, err := txn.Get(bkey)
			if err != nil {
				return err
			}
			err = item.Value(func(val []byte) error {
				return o.PutRaw(hkey, val)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return o, err
}

// Next creates and returns a new DMap from snapshot. It returns ErrLoaderDone when all the DMaps
// are restored from the snapshot.
func (l *Loader) Next() (*DMap, error) {
	for partID, dmaps := range l.dmaps {
		for name := range dmaps {
			var hkeys map[uint64]struct{}
			// Retrieve hkeys which belong to dmap from BadgerDB.
			err := l.s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(dmapKey(partID, name))
				if err != nil {
					return err
				}
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return msgpack.Unmarshal(value, &hkeys)
			})
			if err != nil {
				return nil, err
			}

			// Read raw data from BadgerDB and return an offheap.Offheap
			o, err := l.loadFromBadger(hkeys)
			if err != nil {
				return nil, err
			}
			// Delete processed item.
			delete(l.dmaps[partID], name)
			if len(l.dmaps[partID]) == 0 {
				delete(l.dmaps, partID)
			}
			return &DMap{
				PartID: partID,
				Name:   name,
				Off:    o,
			}, nil
		}
	}
	return nil, ErrLoaderDone
}
