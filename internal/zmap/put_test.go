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

import (
	"context"
	"github.com/buraksezer/olric/internal/protocol"
	"github.com/vmihailenco/msgpack/v5"
	"testing"

	"github.com/buraksezer/olric/internal/resolver"
	"github.com/buraksezer/olric/internal/testzmap"
	"github.com/stretchr/testify/require"
)

var (
	testKey   = []byte("key-1")
	testValue = []byte("value-1")
)

func TestZMap_Transaction_Put(t *testing.T) {
	tz := testzmap.New(t, NewService)
	s := tz.AddStorageNode(nil).(*Service)

	zc := testZMapConfig(t)
	zm, err := s.NewZMap("myzmap", zc)
	require.NoError(t, err)

	tx, err := zm.Transaction(context.Background())
	require.NoError(t, err)

	tx.Put([]byte("key-1"), []byte("value-1"))

	err = tx.Commit()
	require.NoError(t, err)
}

func TestZMap_Transaction_Put_Abort(t *testing.T) {
	tz := testzmap.New(t, NewService)
	s := tz.AddStorageNode(nil).(*Service)

	zc := testZMapConfig(t)
	zm, err := s.NewZMap("myzmap", zc)
	require.NoError(t, err)

	tx1, err := zm.Transaction(context.Background())
	require.NoError(t, err)
	tx1.Put([]byte("key-1"), []byte("value-1"))

	tx2, err := zm.Transaction(context.Background())
	require.NoError(t, err)
	tx2.Put([]byte("key-1"), []byte("value-1"))
	err = tx2.Commit()
	require.NoError(t, err)

	err = tx1.Commit()
	require.ErrorIs(t, err, resolver.ErrTransactionAbort)
}

func TestZMap_TransactionLog_Get(t *testing.T) {
	tz := testzmap.New(t, NewService)
	s := tz.AddStorageNode(nil).(*Service)

	zc := testZMapConfig(t)
	zm, err := s.NewZMap("myzmap", zc)
	require.NoError(t, err)

	tx, err := zm.Transaction(context.Background())
	require.NoError(t, err)

	tx.Put(testKey, testValue)

	err = tx.Commit()
	require.NoError(t, err)

	tlogGetCmd := protocol.NewTransactionLogGet(tx.commitVersion).Command(zm.service.ctx)
	rc := zm.service.client.Get(zm.service.config.Cluster.TransactionLog.Addr)
	err = rc.Process(zm.service.ctx, tlogGetCmd)
	require.NoError(t, err)

	data, err := tlogGetCmd.Bytes()
	require.NoError(t, err)

	var mutations []*mutation
	err = msgpack.Unmarshal(data, &mutations)
	require.NoError(t, err)

	require.Len(t, mutations, 1)
	require.Equal(t, testKey, mutations[0].Key)
	require.Equal(t, testValue, mutations[0].Value)
}
