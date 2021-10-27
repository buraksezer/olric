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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/buraksezer/olric/internal/kvstore/table"
	"github.com/buraksezer/olric/pkg/storage"
)

var (
	startTransportSignature = []byte{0x4F, 0x4C, 0x52, 0x43}
	endTransportSignature   = []byte{0x43, 0x52, 0x4C, 0x4F}
)

// Export serializes underlying data structs into a byte slice. It may return
// ErrFragmented if the tables are fragmented. If you get this error, you should
// try to call Export again some time later.
func (k *KVStore) Export(ctx context.Context) (io.Reader, error) {
	r, w := io.Pipe()

	go func() {
		_, err := w.Write(startTransportSignature)
		if err != nil {
			_ = w.CloseWithError(err)
			return
		}

		for _, t := range k.tables {
			if t.State() == table.RecycledState {
				continue
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			var tableSize [4]byte

			data, err := table.Encode(t)
			if err != nil {
				_ = w.CloseWithError(err)
				return
			}

			binary.BigEndian.PutUint32(tableSize[:], uint32(len(data)))
			_, err = w.Write(tableSize[:])
			if err != nil {
				_ = w.CloseWithError(err)
				return
			}

			_, err = w.Write(data)
			if err != nil {
				_ = w.CloseWithError(err)
				return
			}
		}

		_, err = w.Write(endTransportSignature)
		if err != nil {
			_ = w.CloseWithError(err)
			return
		}
	}()

	return r, nil
}

// Import gets the serialized data by Export and creates a new storage instance.
func (k *KVStore) Import(r io.Reader) (storage.Engine, error) {
	b := make([]byte, len(startTransportSignature))
	_, err := r.Read(b)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(b, startTransportSignature) {
		return nil, errors.New("invalid transport signature")
	}

	c := k.config.Copy()
	child, err := k.EmptyInstance(c)
	if err != nil {
		return nil, err
	}

	for {
		ts := make([]byte, 4)
		_, err = r.Read(ts)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(endTransportSignature, ts) {
			break
		}

		tableSize := binary.BigEndian.Uint32(ts)
		chunk := make([]byte, 4096)
		data := make([]byte, tableSize)
		var readBytes uint32
		for readBytes < tableSize {
			nr, err := r.Read(chunk)
			if err != nil {
				return nil, err
			}
			copy(data[readBytes:], chunk[:nr])
			readBytes += uint32(nr)
		}

		t := table.New(tableSize)
		err = table.Decode(data, t)
		if err != nil {
			return nil, err
		}

		c.Add("tableSize", t.Stats().Allocated)
		child.(*KVStore).AppendTable(t)
	}

	return child, nil
}
