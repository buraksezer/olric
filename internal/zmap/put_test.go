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
	"testing"

	"github.com/buraksezer/olric/internal/testzmap"
	"github.com/stretchr/testify/require"
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
